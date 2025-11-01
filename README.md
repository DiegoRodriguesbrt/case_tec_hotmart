# Estrutura do Projeto

```text
case_tec_hotmart/
├── q1/
│   ├── query_1.sql
│   └── query_2.sql
├── q2/
│   ├── ddl_silver_fct_gmv_diario.sql
│   ├── queries_table.sql
│   ├── sample_silver_fct_gmv_diario.csv
│   └── app/
│       └── main.py
└── README.md
```

## Questão 1

Os scripts SQL referentes à resposta da questão 1 estão localizados na pasta raiz `q1`.

## Questão 2

Todo o desenvolvimento da questão 2 está localizado na pasta raiz q2 com mais detalhes e explicações ao longo desse documento.

## Projeto de ETL para Cálculo de GMV (Gross Merchandising Value)
 
Este repositório contém um script PySpark para um processo de ETL (Extração, Transformação e Carga) que calcula o Gross Merchandising Value (GMV) diário por subsidiária.
 
O projeto foi desenhado para ser robusto, auditável e resiliente a dados que chegam de forma assíncrona ou com atraso, utilizando uma modelagem de dados imutável (Tabela de Snapshots) em um ambiente de Data Lake com Delta Lake.
 
## 🎯 Objetivo
 
O objetivo principal é entregar o valor do **GMV diário por subsidiária**. O GMV é definido como o valor total transacionado, considerando apenas as transações cujo pagamento foi efetuado e que não foram posteriormente canceladas ou reembolsadas.
 
## 🗂️ Fontes de Dados
 
O ETL consome dados de três tabelas de origem localizadas na camada Bronze do Data Lake:
 
1.  **`bronze.purchase`**: Contém os dados principais da transação, como status, datas e valores.
2.  **`bronze.product_item`**: Detalhes sobre os itens de cada compra.
3.  **`bronze.purchase_extra_info`**: Informações adicionais da compra, como a subsidiária.
 
## 🏛️ Modelagem da Tabela Final: `silver.fct_gmv_diario`
 
Para atender aos requisitos de auditoria, rastreabilidade e simplicidade de consulta, a tabela final segue um padrão de **Snapshots Imutáveis** (também conhecido como *Slowly Changing Dimension - Tipo 2*).
 
Em vez de atualizar ou apagar registros, o ETL sempre insere novas linhas para refletir o estado mais recente do GMV de um determinado dia, mantendo um histórico completo de todas as versões.
 
### Estrutura da Tabela
 
| Nome da Coluna | Tipo de Dado | Chave de Partição? | Descrição |
| :--- | :--- | :--- | :--- |
| **`gmv_date`** | `date` | **Sim** | A data de negócio a que o GMV se refere criada com base na coluna release_date. Otimiza as consultas por período. |
| **`subsidiary`** | `string` | Não | Empresa que embora controlada ou dirigida por outra possui grande parte ou o total de suas ações. |
| **`gmv_total_day`** | `double` | Não | O valor total do GMV calculado para o par (`gmv_date`, `subsidiary`). |
| **`calculation_timestamp`** | `timestamp` | Não | O momento exato em que o ETL rodou e gerou esta linha. **É a chave para a "viagem no tempo"**. |
| **`is_latest`** | `boolean` | Não | Um indicador (`true`/`false`) que aponta se esta é a versão mais recente do cálculo. **Simplifica a consulta para o usuário final**. |
 
### Vantagens da Modelagem
 
1.  **Consulta Simples**: Para obter o GMV atual, um usuário final só precisa filtrar por `is_latest = true`.
2.  **Auditoria Completa**: É possível saber exatamente qual era o valor do GMV em qualquer ponto no tempo, consultando pelo `calculation_timestamp`.
3.  **Consistência Histórica**: A visão de um relatório de uma data passada é preservada para sempre, garantindo que os valores retornados por uma consulta histórica nunca mudem.
 
## 🧪 Exemplo de Dataset Final (`silver.fct_gmv_diario`)
 
Abaixo um exemplo fictício (amostra) de como a tabela pode ficar após algumas execuções do ETL. Note que para o mesmo par (`gmv_date`, `subsidiary`) podem existir múltiplas versões (linhas) diferenciadas por `calculation_timestamp`, e apenas uma delas estará com `is_latest = true`.
 
| gmv_date   | subsidiary | gmv_total_day | calculation_timestamp     | is_latest |
|-----------|------------|---------------|---------------------------|-----------|
| 2025-10-28 | nacional        | 152430.55     | 2025-10-29 02:15:07.123   | false     |
| 2025-10-28 | nacional        | 152980.55     | 2025-10-29 06:45:31.847   | true      |
| 2025-10-28 | internacional        |  28450.10     | 2025-10-29 06:45:31.847   | true      |
| 2025-10-29 | nacional        | 167200.10     | 2025-10-30 06:41:12.904   | true      |
| 2025-10-29 | internacional        |  30110.44     | 2025-10-30 06:41:12.904   | true      |
 
Observações:
* A primeira linha (nacional / 2025-10-28) foi substituída por um novo cálculo mais recente (segunda linha) — por isso `is_latest` passou a `false`.
* Se um recálculo futuro ajustar novamente o GMV de 2025-10-28 / nacional, uma terceira linha seria inserida com `is_latest = true` e a linha atualmente `true` seria marcada como `false` via MERGE.
* Consultas de "visão atual" sempre filtram `is_latest = true`.
* Consultas históricas (ponto-no-tempo) usam `calculation_timestamp` para reconstruir o cenário conhecido em uma data passada.
 
> Valores e timestamps são ilustrativos e não representam dados reais.
 
## ⚙️ Lógica do Processo ETL
 
O script `./app/main.py` é estruturado em uma classe `GmvEtlJob` e orquestra o seguinte fluxo:
 
1.  **Extração (Extract)**
    *   O processo é **incremental**. Ele carrega apenas os registros das tabelas de origem que chegaram no dia anterior (**D-1**), filtrando pela `transaction_date`.
 
2.  **Transformação (Transform)**
    *   **Consolidação**: Para cada compra que teve atualização, o ETL busca a versão mais recente de cada registro (`purchase`, `purchase_extra_info`) usando o `transaction_datetime`.
    *   **Junção**: Une as informações de `purchase` e `purchase_extra_info`.
    *   **Lógica de Negócio**: Filtra as compras para incluir apenas as que têm status `APROVADA` e `release_date` preenchida.
    *   **Agregação**: Agrupa os dados por `release_date` e `subsidiary` para calcular o `gmv_total_day`.
    *   **Enriquecimento**: Adiciona as colunas de controle `calculation_timestamp` e `is_latest = true` ao novo snapshot.
 
3.  **Carga (Load)**
    *   Utilizando o comando `MERGE` do Delta Lake, o processo de carga é atômico e seguro.
    *   **Etapa 1 (Desativação)**: O `MERGE` encontra os registros na tabela de destino (`silver.fct_gmv_diario`) que correspondem aos novos cálculos e que estavam marcados como `is_latest = true`. Esses registros são atualizados para `is_latest = false`.
    *   **Etapa 2 (Inserção)**: Os novos snapshots de GMV, já com `is_latest = true`, são inseridos na tabela.
 
## 🔍 Como Consultar os Dados
 
### Consultando o GMV Atual (Visão Simplificada)
 
Para obter a visão mais recente e correta do GMV, utilize o flag `is_latest`.
 
```sql
SELECT
  gmv_date,
  subsidiary,
  gmv_total_day
FROM silver.fct_gmv_diario
WHERE is_latest = true
  AND gmv_date BETWEEN '2023-01-01' AND '2023-01-31';
```
 
### Consultando o GMV em um Ponto no Tempo (Viagem no Tempo)
 
Para saber qual era o valor do GMV de Janeiro/2023 na data de 31/03/2023, utilize o `calculation_timestamp`.
 
```sql
WITH ranked_history AS (
  SELECT
    gmv_date,
    subsidiary,
    gmv_total_day,
    ROW_NUMBER() OVER(PARTITION BY gmv_date, subsidiary ORDER BY calculation_timestamp DESC) as rn
  FROM silver.fct_gmv_diario
  WHERE calculation_timestamp <= '2023-03-31 23:59:59'
    AND gmv_date BETWEEN '2023-01-01' AND '2023-01-31'
)
SELECT
  gmv_date,
  subsidiary,
  gmv_total_day
FROM ranked_history
WHERE rn = 1;
```
Este tipo de consulta garante que o valor retornado será **exatamente** o que era conhecido na data especificada, de forma consistente e imutável.
 
---
## Nota sobre a Arquitetura
 
A tabela `product_item` é extraída, mas não é usada diretamente no cálculo do GMV.
 
*   **Eficiência**: O cálculo atual usa o campo `purchase_total_value` da tabela `purchase`, que é mais direto para obter o valor total da compra.
*   **Flexibilidade**: Manter a extração de `product_item` prepara o ETL para futuras análises em nível de produto, sem a necessidade de grandes alterações no código.
