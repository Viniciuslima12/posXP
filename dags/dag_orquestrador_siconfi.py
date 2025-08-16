from __future__ import annotations
import pandas as pd
from datetime import datetime

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.models.param import Param

# Importando as funções dos jobs separados
from include.jobs.extracaoEntesSICONFI import extrair_entes_df
from include.jobs.extracaoRGFSICONFI import extrair_dados_rgf
from include.jobs.loader_dados import salvar_dataframe

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
        "camada": Param("bronze", type="string", enum=["bronze", "silver", "gold"], title="Camada de Armazenamento"),
        "formato": Param("csv", type="string", enum=["csv", "parquet"], title="Formato de Saída"),
        "rgf_uf": Param(
            "MS", 
            type="string", 
            title="[RGF] UF para Filtro",
            enum=["MS", "SP", "RJ", "MT"], # <-- MUDANÇA: Adicionado menu de seleção
        ),
        "rgf_ano": Param(2024, type="integer", title="[RGF] Ano de Referência"),
        "rgf_ibge_code": Param(
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
        s3_key = f"{p['camada']}/siconfi/entes.{p['formato']}"

        caminho_final = salvar_dataframe(
            df=df_para_salvar,
            destino=p['destino'],
            formato=p['formato'],
            s3_conn_id=p['s3_conn_id'],
            s3_bucket=p['s3_bucket'],
            s3_key=s3_key,
            # Parâmetros locais, caso o destino seja 'local'
            local_path=f"Dados/{p['camada']}SICONFI",
            local_filename="entes"
        )
        return caminho_final

    # --- ETAPA 2: RGF ---
    @task(task_id="extrair_rgf")
    def task_extrair_rgf(caminho_arquivo_entes: str, **kwargs) -> pd.DataFrame:
        p = kwargs['params']
        return extrair_dados_rgf(
            caminho_arquivo_entes=caminho_arquivo_entes,
            ano_referencia=p['rgf_ano'],
            uf_filtro=p['rgf_uf'], # Passando a UF selecionada
            ibge_filtro=p['rgf_ibge_code']
        )
    @task(task_id="salvar_rgf")
    def task_salvar_rgf(df_para_salvar: pd.DataFrame, **kwargs):
        p = kwargs['params']
        nome_arquivo = f"MS_{p['rgf_ano']}_rgf" # Nome do arquivo de saída
        s3_key = f"brutos/siconfi/rgf/{kwargs['ds_nodash']}/{nome_arquivo}.{p['formato']}"

        return salvar_dataframe(
            df=df_para_salvar, destino=p['destino'], formato=p['formato'],
            s3_conn_id=p['s3_conn_id'], s3_bucket=p['s3_bucket'], s3_key=s3_key,
            local_path="Dados/bronzeSICONFI/CampoGrande", local_filename=nome_arquivo
        )
    
    
    # 1. Extrai Entes
    df_entes = task_extrair_entes()
    # 2. Salva Entes, e o caminho do arquivo é passado para a próxima etapa
    caminho_entes_salvo = task_salvar_entes(df_para_salvar=df_entes)
    
    # 3. Extrai RGF, que depende do arquivo de entes ter sido salvo
    df_rgf = task_extrair_rgf(caminho_arquivo_entes=caminho_entes_salvo)
    # 4. Salva RGF, que depende do DataFrame do RGF ter sido extraído
    caminho_rgf_salvo = task_salvar_rgf(df_para_salvar=df_rgf)