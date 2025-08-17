import requests
import os
import pandas as pd
import datetime
import re

from include.jobs.loader_dados import salvar_dataframe


def chama_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()  # ou response.text se não for JSON
    else:
        print(f"Erro ao acessar a API: {response.status_code}")
        return None
    
def salva_arquivo(df, caminho_arquivo, formato='parquet'):
    if formato == 'parquet':
        df.to_parquet(caminho_arquivo, compression='brotli', index=False)
    elif formato == 'xlsx':
        df.to_excel(caminho_arquivo, index=False)
    elif formato == 'csv':
        df.to_csv(caminho_arquivo, index=False)
    else:
        raise ValueError("Formato não suportado. Use 'parquet', 'excel' ou 'csv'.")

def pega_nomeURL(url):
    # Pega o caminho antes do '?'
    caminho = url.split('?')[0]
    
    # Pega o último segmento do caminho
    nome_recurso = caminho.split('/')[-1]
    
    return nome_recurso

def salva_log(logs, caminho_arquivo="log_extracao.csv"):
    if not os.path.exists(caminho_arquivo):
        pd.DataFrame(logs).to_csv(caminho_arquivo, index=False)
    else:
        pd.DataFrame(logs).to_csv(caminho_arquivo, mode='a', header=False, index=False)

def gerar_ultimos_anos(ano_final: int = None) -> list[int]:
    """
    Gera uma lista com os últimos 5 anos terminando em um ano específico.

    Args:
        ano_final (int, optional): O último ano da sequência. 
                                   Se não for fornecido, o ano atual será usado como padrão.

    Returns:
        list[int]: Uma lista de inteiros contendo os 5 anos em ordem crescente.
    """
    # 1. Verifica se o ano_final foi fornecido. Se não, usa o ano atual.
    if ano_final is None:
        ano_final = datetime.datetime.now().year
    
    # 2. Calcula o ano inicial da sequência (5 anos atrás)
    ano_inicial = ano_final - 4
    
    # 3. Gera a lista de anos usando um range e a retorna
    lista_de_anos = list(range(ano_inicial, ano_final + 1))
    
    return lista_de_anos        

def _sanitizar_nome_arquivo(nome: str) -> str:
    """Função auxiliar para limpar o nome do anexo e torná-lo um nome de arquivo válido."""
    nome = re.sub(r'[^\w\s-]', '', nome).strip()
    nome = re.sub(r'[-\s]+', '_', nome)
    return nome


def transformar_bronze_para_silver_por_anexo(
    caminho_arquivo_bronze: str,
    destino_saida: str,
    formato_saida: str,
    path_saida_silver_local_base: str,
    s3_conn_id: str,
    s3_bucket: str,
    s3_path_saida_silver_base: str,
    ds_nodash: str,
    aws_credentials
) -> list:
    """
    Lê um arquivo consolidado da camada Bronze (RGF ou RREO), itera pela coluna 'anexo',
    e salva um arquivo separado para cada anexo na camada Silver.
    """
    print(f"--- INICIANDO TRANSFORMAÇÃO BRONZE -> SILVER ---")
    print(f"Lendo arquivo de origem: {caminho_arquivo_bronze}")

    storage_options = {
        "key": aws_credentials.access_key,
        "secret": aws_credentials.secret_key,
        "token": aws_credentials.token,
    }
    
    try:
        df_bronze = pd.read_parquet(caminho_arquivo_bronze, storage_options=storage_options)
    except Exception:
        print("Arquivo de origem vazio ou inválido. Nenhuma transformação será feita.")
        return []

    if df_bronze.empty or 'anexo' not in df_bronze.columns:
        print("DataFrame de origem vazio ou sem a coluna 'anexo'. A transformação será pulada.")
        return []

    anexos_unicos = df_bronze['anexo'].unique()
    print(f"Encontrados {len(anexos_unicos)} anexos únicos para processar.")
    
    arquivos_silver_criados = []

    for anexo in anexos_unicos:
        if anexo is None:
            print("Pulando registros com anexo nulo.")
            continue
            
        print(f"Processando anexo: '{anexo}'")
        df_anexo = df_bronze[df_bronze['anexo'] == anexo]
        
        nome_arquivo_anexo = _sanitizar_nome_arquivo(anexo)
        
        # Chama a função de salvar, que agora está neste mesmo arquivo
        caminho_final_anexo = salvar_dataframe(
            df=df_anexo,
            destino=destino_saida,
            formato=formato_saida,
            s3_conn_id=s3_conn_id,
            s3_bucket=s3_bucket,
            s3_key=f"{s3_path_saida_silver_base}/{ds_nodash}/{nome_arquivo_anexo}.{formato_saida}",
            local_path=path_saida_silver_local_base,
            local_filename=nome_arquivo_anexo
        )
        arquivos_silver_criados.append(caminho_final_anexo)

    print(f"--- TRANSFORMAÇÃO CONCLUÍDA ---")
    return arquivos_silver_criados


def ler_arquivo_parquet(caminho_do_arquivo: str, aws_credentials=None) -> pd.DataFrame:
    """
    Lê um arquivo Parquet de forma inteligente, seja de um caminho local ou de um bucket S3.

    Args:
        caminho_do_arquivo (str): O caminho completo (ex: 'Dados/arquivo.parquet' ou 's3://bucket/key.parquet').
        aws_credentials (boto3.session.Session.credentials, optional): Credenciais da AWS.
                                                                       Necessário apenas se o caminho for S3.

    Returns:
        pd.DataFrame: O DataFrame lido do arquivo.
    """
    print(f"Lendo arquivo Parquet de: {caminho_do_arquivo}")

    if caminho_do_arquivo.startswith('s3://'):
        # Se o caminho é S3, verifica se as credenciais foram fornecidas
        if not aws_credentials:
            raise ValueError("Credenciais da AWS são necessárias para ler de um caminho S3.")
        
        print("Caminho S3 detectado. Usando credenciais para leitura.")
        storage_options = {
            "key": aws_credentials.access_key,
            "secret": aws_credentials.secret_key,
            "token": aws_credentials.token,
        }
        df = pd.read_parquet(caminho_do_arquivo, storage_options=storage_options)
        print("Arquivo lido com sucesso do S3.")

    else:
        # Se o caminho não começa com s3://, assume que é local
        print("Caminho local detectado. Lendo do sistema de arquivos.")
        df = pd.read_parquet(caminho_do_arquivo)
        print("Arquivo lido com sucesso do sistema de arquivos local.")
    
    return df