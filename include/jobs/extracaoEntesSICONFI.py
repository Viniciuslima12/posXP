import pandas as pd
from include.functions import chama_api

def extrair_entes_df(url: str, tabela_json: str) -> pd.DataFrame:
    """
    Busca dados de uma API e os converte para um Pandas DataFrame.

    Retorna um DataFrame.
    """
    print(f"Iniciando extração da URL: {url}")
    dados_api = chama_api(url)

    if not dados_api or tabela_json not in dados_api:
        raise ValueError(f"A chave '{tabela_json}' não foi encontrada na resposta da API.")

    content = dados_api.get(tabela_json, [])
    df = pd.DataFrame(content)
    
    print(f"Extração concluída. {len(df)} registros encontrados.")
    return df