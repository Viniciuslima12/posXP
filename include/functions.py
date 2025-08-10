import requests
import os
import pandas as pd
import datetime


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