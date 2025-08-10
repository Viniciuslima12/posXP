from __future__ import annotations
import pandas as pd
from datetime import datetime

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.models.param import Param

# Importando as funções dos jobs separados
from include.jobs.extracaoEntesSICONFI import extrair_para_dataframe
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
    }
) as dag:

    @task(task_id="extrair_dados_siconfi")
    def task_extrair() -> pd.DataFrame:
        """
        Tarefa de Extração: Chama o job extrator e retorna um DataFrame.
        O Airflow passará este DataFrame para a próxima tarefa via XCom.
        """
        url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/entes"
        tabela_json = "items"
        
        df_extraido = extrair_para_dataframe(url=url, tabela_json=tabela_json)
        return df_extraido

    @task(task_id="salvar_dados")
    def task_salvar(df_para_salvar: pd.DataFrame, **kwargs):
        """
        Tarefa de Carga: Recebe um DataFrame e chama o job carregador.
        """
        params = kwargs['params']
        p = params # Alias

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

    # --- Definindo a Orquestração do Fluxo ---
    dataframe_extraido = task_extrair()
    resultado_salvamento = task_salvar(df_para_salvar=dataframe_extraido)