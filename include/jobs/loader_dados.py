import pandas as pd
import os
from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def salvar_dataframe(
    df: pd.DataFrame,
    destino: str,
    formato: str,
    # Parâmetros para 'local'
    local_path: str = None,
    local_filename: str = None,
    # Parâmetros para 's3'
    s3_conn_id: str = None,
    s3_bucket: str = None,
    s3_key: str = None
) -> str:
    """
    Recebe um DataFrame e o salva em um destino especificado ('local' ou 's3').

    Retorna uma string com o caminho final onde o arquivo foi salvo.
    """
    print(f"Loader iniciado. Destino: {destino.upper()}, Formato: {formato.upper()}.")
    
    if df.empty:
        print("DataFrame vazio. Nenhum arquivo será salvo.")
        return "DataFrame vazio, salvamento pulado."

    if destino == 'local':
        if not local_path or not local_filename:
            raise ValueError("Para destino 'local', 'local_path' e 'local_filename' são obrigatórios.")
        os.makedirs(local_path, exist_ok=True)
        caminho_final = f"{local_path}/{local_filename}.{formato}"
        
        if formato == 'csv':
            df.to_csv(caminho_final, index=False)
        elif formato == 'parquet':
            df.to_parquet(caminho_final, index=False)
        else:
            raise ValueError(f"Formato '{formato}' não suportado para destino local.")

    elif destino == 's3':
        if not s3_conn_id or not s3_bucket or not s3_key:
            raise ValueError("Para destino 's3', 's3_conn_id', 's3_bucket' e 's3_key' são obrigatórios.")
        
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        
        if formato == 'csv':
            print("Convertendo DataFrame para string CSV...")
            dados_string = df.to_csv(index=False)
            print("Enviando para o S3 usando load_string...")
            s3_hook.load_string(string_data=dados_string, key=s3_key, bucket_name=s3_bucket, replace=True)
            caminho_final = f"s3://{s3_bucket}/{s3_key}"
        
        elif formato == 'parquet':
            print("Convertendo DataFrame para bytes Parquet...")
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            dados_bytes = buffer.getvalue()
            print("Enviando para o S3 usando load_bytes...")
            s3_hook.load_bytes(bytes_data=dados_bytes, key=s3_key, bucket_name=s3_bucket, replace=True)
            caminho_final = f"s3://{s3_bucket}/{s3_key}"
            
        else:
            raise ValueError(f"Formato '{formato}' não suportado para destino S3.")


    else:
        raise ValueError(f"Destino '{destino}' é inválido. Use 'local' ou 's3'.")

    print(f"DataFrame com {len(df)} linhas salvo com sucesso em: {caminho_final}")
    return caminho_final