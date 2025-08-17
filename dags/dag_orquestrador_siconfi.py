from __future__ import annotations
import pandas as pd
from datetime import datetime

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Importando as funções dos jobs separados
from include.jobs.extracaoEntesSICONFI import extrair_entes_df
from include.jobs.extracaoRGFSICONFI import extrair_dados_rgf
from include.jobs.extracaoRREOSICONFI import extrair_dados_rreo
from include.jobs.loader_dados import salvar_dataframe

# Funções genéricas de transformação
from include.functions import transformar_bronze_para_silver_por_anexo

ID_S3 = "amazon_s3" 

with DAG(
    dag_id='dag_extracao_siconfi_modular',
    start_date=datetime(2025, 8, 10),
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ### DAG Modular de Extração SICONFI
    Esta DAG demonstra um padrão de 2 etapas (Extração e Carga),
    onde cada etapa é uma função em um arquivo separado.
    """,
    tags=['siconfi', 'modular', 'best-practice'],
    
    params={
        "destino": Param("s3", type="string", title="Destino dos Dados", enum=["s3", "local"]),
        "s3_conn_id": Param(ID_S3, type="string", title="[S3] Airflow Connection ID"),
        "s3_bucket": Param("dados-projeto-aplicado", type="string", title="[S3] Nome do Bucket"),
        # "camada": Param("bronze", type="string", enum=["bronze", "silver", "gold"], title="Camada de Armazenamento"),
        "formato": Param("parquet", type="string", enum=["csv", "parquet"], title="Formato de Saída"),
        "uf": Param(
            "MS", 
            type="string", 
            title="[RGF] UF para Filtro",
            enum=["MS", "SP", "RJ", "MT"], # <-- MUDANÇA: Adicionado menu de seleção
        ),
        "ano": Param(2024, type="integer", title="[RGF] Ano de Referência"),
        "ibge_code": Param(
            "5002704", 
            type="string", # <-- MUDANÇA: Alterado para string para aceitar 'TODOS'
            title="[RGF] Código IBGE ou 'TODOS'",
            description="Digite um código IBGE específico ou a palavra TODOS para processar o estado inteiro."
        )
    }
) as dag:

    @task(task_id="extrair_entes")
    def task_extrair_entes() -> pd.DataFrame:
        """
        Tarefa de Extração: Chama o job extrator e retorna um DataFrame.
        O Airflow passará este DataFrame para a próxima tarefa via XCom.
        """
        url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/entes"
        tabela_json = "items"
        
        return extrair_entes_df(url=url, tabela_json=tabela_json)

    @task(task_id="salvar_entes")
    def task_salvar_entes(df_para_salvar: pd.DataFrame, **kwargs) -> str:
        """
        Tarefa de Carga: Recebe um DataFrame e chama o job carregador.
        """
        p = kwargs['params']

        # Monta a chave do S3 dinamicamente
        s3_key = f"bronze/siconfi/entes.{p['formato']}"

        caminho_final = salvar_dataframe(
            df=df_para_salvar,
            destino=p['destino'],
            formato=p['formato'],
            s3_conn_id=p['s3_conn_id'],
            s3_bucket=p['s3_bucket'],
            s3_key=s3_key,
            # Parâmetros locais, caso o destino seja 'local'
            local_path=f"Dados/bronzeSICONFI",
            local_filename="entes"
        )
        return caminho_final

    # --- ETAPA 2: RGF ---
    @task(task_id="extrair_rgf")
    def task_extrair_rgf(caminho_arquivo_entes: str, **kwargs) -> pd.DataFrame:
        p = kwargs['params']
        aws_credentials = None
        # Só busca as credenciais da AWS se o destino for 's3'
        if p['destino'] == 's3':
            print(f"Destino é S3. Buscando credenciais da conexão: {p['s3_conn_id']}")
            hook = S3Hook(aws_conn_id=p['s3_conn_id'])
            aws_credentials = hook.get_credentials()
        else:
            print("Destino é local. Não buscará credenciais da AWS para a leitura.")

        # 2. Passa as credenciais para a função de extração
        return extrair_dados_rgf(
            caminho_arquivo_entes=caminho_arquivo_entes,
            ano_referencia=p['ano'],
            uf_filtro=p['uf'],
            ibge_filtro=p['ibge_code'],
            aws_credentials=aws_credentials 
        )
    
    @task(task_id="salvar_rgf")
    def task_salvar_rgf(df_para_salvar: pd.DataFrame, **kwargs):
        p = kwargs['params']
        nome_arquivo = f"MS_{p['ano']}_rgf" # Nome do arquivo de saída
        s3_key = f"bronze/siconfi/rgf/{nome_arquivo}.{p['formato']}"

        return salvar_dataframe(
            df=df_para_salvar, destino=p['destino'], formato=p['formato'],
            s3_conn_id=p['s3_conn_id'], s3_bucket=p['s3_bucket'], s3_key=s3_key,
            local_path="Dados/bronzeSICONFI", local_filename=nome_arquivo
        )
    
    # --- ETAPA 3: RREO ---
    @task(task_id="extrair_rreo")
    def task_extrair_rreo(caminho_arquivo_entes: str, **kwargs) -> pd.DataFrame:
        p = kwargs['params']
        aws_credentials = None
        # Só busca as credenciais da AWS se o destino for 's3'
        if p['destino'] == 's3':
            print(f"Destino é S3. Buscando credenciais da conexão: {p['s3_conn_id']}")
            hook = S3Hook(aws_conn_id=p['s3_conn_id'])
            aws_credentials = hook.get_credentials()
        else:
            print("Destino é local. Não buscará credenciais da AWS para a leitura.")

        return extrair_dados_rreo(
            caminho_arquivo_entes=caminho_arquivo_entes,
            ano_referencia=p['ano'],
            uf_filtro=p['uf'], # Passando a UF selecionada
            ibge_filtro=p['ibge_code'],
            aws_credentials=aws_credentials
        )
    @task(task_id="salvar_rreo")
    def task_salvar_rreo(df_para_salvar: pd.DataFrame, **kwargs):
        p = kwargs['params']
        nome_arquivo = f"MS_{p['ano']}_rreo" # Nome do arquivo de saída
        s3_key = f"bronze/siconfi/rreo/{nome_arquivo}.{p['formato']}"

        return salvar_dataframe(
            df=df_para_salvar, destino=p['destino'], formato=p['formato'],
            s3_conn_id=p['s3_conn_id'], s3_bucket=p['s3_bucket'], s3_key=s3_key,
            local_path="Dados/bronzeSICONFI", local_filename=nome_arquivo
        )
    
    @task(task_id="processar_rgf_para_silver")
    def task_processar_rgf_silver(caminho_arquivo_bronze: str, **kwargs):
        p = kwargs['params']
        
        # Buscando as credenciais para passar para o transformador
        hook = S3Hook(aws_conn_id=p['s3_conn_id'])
        aws_credentials = hook.get_credentials()
        
        lista_arquivos_criados = transformar_bronze_para_silver_por_anexo(
            caminho_arquivo_bronze=caminho_arquivo_bronze,
            destino_saida=p['destino'],
            formato_saida=p['formato'],
            path_saida_silver_local_base="Dados/silverSICONFI", # Novo caminho local
            s3_conn_id=p['s3_conn_id'],
            s3_bucket=p['s3_bucket'],
            s3_path_saida_silver_base="silver/siconfi/rgf", # Novo caminho S3
            ds_nodash=kwargs['ds_nodash'],
            aws_credentials=aws_credentials
        )
        return lista_arquivos_criados
        
    
    
    # 1. Extrai Entes
    df_entes = task_extrair_entes()
    # 2. Salva Entes, e o caminho do arquivo é passado para a próxima etapa
    caminho_entes_salvo = task_salvar_entes(df_para_salvar=df_entes)
    
    # 3. Extrai RGF, que depende do arquivo de entes ter sido salvo
    df_rgf = task_extrair_rgf(caminho_arquivo_entes=caminho_entes_salvo)
    # 4. Salva RGF, que depende do DataFrame do RGF ter sido extraído
    caminho_rgf_bronze = task_salvar_rgf(df_para_salvar=df_rgf)

    # 5. Extrai RREO, que depende do arquivo de entes ter sido salvo
    df_rgf = task_extrair_rreo(caminho_arquivo_entes=caminho_entes_salvo)
    # 46. Salva RREO, que depende do DataFrame do RGF ter sido extraído
    caminho_rreo_bronze = task_salvar_rreo(df_para_salvar=df_rgf)

    # Camada Silver
    lista_arquivos_silver_rgf = task_processar_rgf_silver(caminho_arquivo_bronze=caminho_rgf_bronze)