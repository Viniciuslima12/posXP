# Projeto Pipeline SICONFI

Este projeto implementa um pipeline de dados utilizando Apache Airflow para extração, transformação e carga (ETL) de dados do SICONFI, com organização modular e suporte a múltiplas camadas (bronze, silver, gold).



## Descrição dos Principais Arquivos

### dags/dag_orquestrador_siconfi.py

- DAG principal do Airflow que orquestra todo o fluxo de ETL, desde a extração dos entes até a geração dos arquivos gold.
- Utiliza tasks modulares e funções importadas do pacote `include.jobs`.

### include/config.py

- Arquivo de configuração com parâmetros globais, como nome da tabela principal no JSON e formato padrão de saída.

### include/functions.py

- Funções utilitárias para:
  - Chamada de APIs (`chama_api`)
  - Leitura inteligente de arquivos Parquet locais ou S3 (`ler_arquivo_parquet`)
  - Transformações auxiliares e sanitização de nomes de arquivos
  - Função genérica para transformar bronze em silver por anexo

### include/jobs/extracaoEntesSICONFI.py

- Função [`extrair_entes_df`](include/jobs/extracaoEntesSICONFI.py) para extrair a lista de entes do SICONFI via API e converter em DataFrame.

### include/jobs/extracaoRGFSICONFI.py

- Função [`extrair_dados_rgf`](include/jobs/extracaoRGFSICONFI.py) para extrair dados do RGF de municípios, filtrando por UF e código IBGE, consolidando os dados por trimestre.

### include/jobs/extracaoRREOSICONFI.py

- Função [`extrair_dados_rreo`](include/jobs/extracaoRREOSICONFI.py) para extrair dados do RREO, também com filtros por UF e IBGE, consolidando por bimestre.

### include/jobs/loader_dados.py

- Função [`salvar_dataframe`](include/jobs/loader_dados.py) para salvar DataFrames localmente ou em buckets S3, nos formatos CSV ou Parquet.

### include/jobs/transformer_gold.py

- Funções para transformar arquivos silver em gold:
  - [`transformar_rgf_silver_para_gold`](include/jobs/transformer_gold.py): indicadores de pessoal e receita do RGF.
  - [`transformar_rreo_silver_para_gold`](include/jobs/transformer_gold.py): indicadores de receitas e despesas do RREO.

## Como Executar

1. Configure as variáveis e conexões necessárias no Airflow.
2. Execute a DAG `dag_extracao_siconfi_modular` via UI ou CLI do Airflow.
3. Os dados serão processados e salvos nas camadas bronze, silver e gold, conforme parametrização.

---

Para mais detalhes sobre cada função, consulte os arquivos correspondentes nos diretórios [dags/](dags/dag_orquestrador_siconfi.py) e [include/jobs/](include/jobs/).