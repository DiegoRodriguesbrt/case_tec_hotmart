"""
Módulo de ETL para cálculo diário do GMV (Gross Merchandising Value).

Este script implementa um processo de ETL para calcular o GMV diário por subsidiária,
seguindo uma abordagem de modelagem imutável (Slowly Changing Dimension - Tipo 2)
para garantir a rastreabilidade e a consistência histórica dos dados.

O processo é incremental, processando apenas os dados que chegaram no dia anterior (D-1).
"""

import logging
import sys
import uuid
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, sum as _sum, lit, current_timestamp, row_number, date_sub
from delta.tables import DeltaTable


class Logger:
    """Classe para configurar um logger padronizado."""
    @staticmethod
    def configure_logging():
        logger = logging.getLogger("GmvEtlJob")
        correlation_id = str(uuid.uuid4())
        log_format = f"%(asctime)s | {correlation_id} | %(levelname)s | %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"
        log_stream = sys.stdout
        if logger.handlers:
            for handler in logger.handlers:
                logger.removeHandler(handler)
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            stream=log_stream,
            datefmt=date_format
        )
        return logger


class GmvEtlJob:
    """
    Encapsula a lógica do processo de ETL para o cálculo do GMV.
    """

    def __init__(self, app_name: str = "GMV_Daily_ETL_Immutable"):
        """
        Inicializa o job de ETL, configurando o logger e a sessão Spark.
        """
        self.logger = Logger.configure_logging()
        self.spark = self._create_spark_session(app_name)
        self.processing_date = self._get_processing_date()

    def _create_spark_session(self, app_name: str) -> SparkSession:
        """Cria e configura uma SparkSession com suporte para Delta Lake."""
        self.logger.info("Inicializando Spark Session...")
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .enableHiveSupport() \
            .getOrCreate()

    def _get_processing_date(self) -> str:
        """Retorna a data de processamento (D-1) como uma string."""
        date = str(self.spark.range(1).select(
            date_sub(current_timestamp(), 1).cast('date')).first()[0])
        self.logger.info(f"Data de processamento definida para: {date}")
        return date

    def _get_latest_records(self, df: DataFrame, partition_keys: list, order_key: str) -> DataFrame:
        """Função auxiliar para encontrar a versão mais recente de cada registro."""
        window_spec = Window.partitionBy(
            partition_keys).orderBy(col(order_key).desc())
        return df.withColumn("rn", row_number().over(window_spec)) \
                 .filter(col("rn") == 1) \
                 .drop("rn")

    def extract_source_data(self) -> tuple[DataFrame, DataFrame, DataFrame]:
        """Carrega os dados de origem do dia de processamento."""
        self.logger.info(
            f"1. Extraindo dados de origem para a data: {self.processing_date}...")
        purchase_df = self.spark.table("bronze.purchase").filter(
            col("transaction_date") == self.processing_date)
        product_item_df = self.spark.table("bronze.product_item").filter(
            col("transaction_date") == self.processing_date)
        purchase_extra_info_df = self.spark.table("bronze.purchase_extra_info").filter(
            col("transaction_date") == self.processing_date)
        return purchase_df, product_item_df, purchase_extra_info_df

    def transform_gmv_snapshot(self, purchase_df: DataFrame, purchase_extra_info_df: DataFrame) -> DataFrame:
        """Realiza a transformação dos dados para calcular o snapshot do GMV diário."""
        self.logger.info(
            "2. Transformando dados para calcular o snapshot do GMV...")

        self.logger.info(
            "2.1. Consolidando o estado mais recente de cada tabela de origem...")
        latest_purchase = self._get_latest_records(
            purchase_df, ["purchase_id", "purchase_partition"], "transaction_datetime")
        latest_purchase_info = self._get_latest_records(purchase_extra_info_df, [
                                                        "purchase_id", "purchase_partition"], "transaction_datetime")

        self.logger.info("2.2. Juntando as versões mais recentes dos dados...")
        joined_df = latest_purchase.join(
            latest_purchase_info, ["purchase_id", "purchase_partition"], "left")

        self.logger.info(
            "2.3. Filtrando compras elegíveis e calculando o GMV diário...")
        gmv_eligible_purchases = joined_df.filter(
            (col("release_date").isNotNull()) & (
                col("purchase_status") == "APROVADA")
        )

        daily_gmv = gmv_eligible_purchases.groupBy("release_date", "subsidiary") \
            .agg(_sum("purchase_total_value").alias("gmv_total_day")) \
            .withColumnRenamed("release_date", "gmv_date")

        self.logger.info(
            "2.4. Adicionando colunas de controle para a modelagem imutável...")
        new_gmv_snapshot = daily_gmv.withColumn("calculation_timestamp", current_timestamp()) \
                                    .withColumn("is_latest", lit(True))

        self.logger.info(
            "Novos cálculos de GMV prontos para serem integrados:")
        new_gmv_snapshot.show(5, truncate=False)
        return new_gmv_snapshot

    def load_data_to_delta(self, df_to_load: DataFrame, target_table_name: str):
        """Carrega o DataFrame na tabela Delta de destino com lógica de imutabilidade."""
        self.logger.info(
            f"3. Carregando dados na tabela de destino '{target_table_name}'...")

        if df_to_load.rdd.isEmpty():
            self.logger.warning(
                "Nenhum novo snapshot de GMV para carregar. Finalizando etapa de carga.")
            return

        if not DeltaTable.isDeltaTable(self.spark, target_table_name):
            self.logger.info(
                f"Tabela de destino '{target_table_name}' não encontrada. Criando pela primeira vez.")
            df_to_load.write.format("delta").partitionBy(
                "gmv_date").saveAsTable(target_table_name)
        else:
            self.logger.info(
                f"Tabela de destino '{target_table_name}' encontrada. Iniciando processo de MERGE.")
            target_table = DeltaTable.forName(self.spark, target_table_name)

            self.logger.info("Desativando registros que serão atualizados...")
            target_table.alias("target").merge(
                source=df_to_load.alias("source"),
                condition="target.gmv_date = source.gmv_date AND target.subsidiary = source.subsidiary AND target.is_latest = true"
            ).whenMatchedUpdate(set={"is_latest": lit(False)}).execute()

            self.logger.info("Inserindo novos registros calculados...")
            df_to_load.write.format("delta").mode(
                "append").saveAsTable(target_table_name)

    def run(self):
        """Orquestra a execução completa do job de ETL."""
        self.logger.info("Iniciando job de ETL do GMV.")
        try:
            purchase_df, _, purchase_extra_info_df = self.extract_source_data()
            new_gmv_snapshot = self.transform_gmv_snapshot(
                purchase_df, purchase_extra_info_df)
            self.load_data_to_delta(new_gmv_snapshot, "silver.fct_gmv_diario")
            self.logger.info("Job de ETL do GMV finalizado com sucesso!")
        except Exception as e:
            self.logger.error(
                f"Ocorreu um erro durante a execução do ETL: {e}", exc_info=True)
            raise

    def stop(self):
        """Para a sessão Spark."""
        if self.spark:
            self.logger.info("Finalizando Spark Session.")
            self.spark.stop()


if __name__ == "__main__":
    job = None
    try:
        job = GmvEtlJob()
        job.run()
    except Exception as e:
        print(f"Execução do job falhou. Verifique os logs.", file=sys.stderr)
    finally:
        if job:
            job.stop()
