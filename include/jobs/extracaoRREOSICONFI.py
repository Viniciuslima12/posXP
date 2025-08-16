import pandas as pd
import time
from datetime import datetime

# Importando as funções helper de baixo nível e o nosso loader reutilizável
from include.functions import chama_api, salva_log
from include.jobs.loader_dados import salvar_dataframe

def extrair_dados_rgf(
    caminho_arquivo_entes: str,
    ano_referencia: int,
    uf_filtro: str, # <-- Novo parâmetro para filtrar o estado
    ibge_filtro: str,
) -> pd.DataFrame:
    """
    Executa a extração de dados do RGF para um município específico.
    
    1. Lê a lista de entes de um arquivo parquet (local ou S3).
    2. Filtra pelo município desejado.
    3. Itera pelos períodos, chama a API do RGF e consolida os dados.
    4. Salva o resultado consolidado usando a função genérica salvar_dataframe.
    """
    print("--- INICIANDO JOB DE EXTRAÇÃO SICONFI: RGF ---")
    print(f"Lendo arquivo de entes de: {caminho_arquivo_entes}")

    # Pandas lê nativamente de S3 se as bibliotecas s3fs e pyarrow estiverem instaladas
    df_entes = pd.read_parquet(caminho_arquivo_entes)
    
    # --- LÓGICA DE FILTRAGEM ATUALIZADA ---
    print(f"Filtrando entes para a UF: {uf_filtro}")
    entes_do_estado = df_entes[df_entes['uf'] == uf_filtro]


    
    entes_a_processar = pd.DataFrame()
    if str(ibge_filtro).upper() == 'TODOS':
        print(f"Processando TODOS os {len(entes_do_estado)} entes de {uf_filtro}.")
        entes_a_processar = entes_do_estado
    else:
        try:
            # Converte o código IBGE (que vem como string) para número
            ibge_codigo_num = int(ibge_filtro)
            print(f"Filtrando pelo código IBGE específico: {ibge_codigo_num}")
            entes_a_processar = entes_do_estado[entes_do_estado['cod_ibge'] == ibge_codigo_num]
        except ValueError:
            raise ValueError(f"O código IBGE '{ibge_filtro}' fornecido não é um número válido.")

    if entes_a_processar.empty:
        raise ValueError(f"Nenhum ente encontrado para os filtros (UF: {uf_filtro}, IBGE: {ibge_filtro})")

    # --- Lógica de extração (loop) ---
    base_url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rgf"
    # ... (demais variáveis de configuração inalteradas) ...
    dados_consolidados_ano = pd.DataFrame()

    print(f"Processando RGF para o ano: {ano_referencia}. Total de entes na fila: {len(entes_a_processar)}")
    
    # O loop agora itera sobre a lista filtrada (que pode ter 1 ou N municípios)
    for i in entes_a_processar.index:
        ente_ibge = entes_a_processar.loc[i, 'cod_ibge']
        ente_nome = entes_a_processar.loc[i, 'ente']
        print(f"\nProcessando ente: {ente_nome} ({ente_ibge})")
        
        for trimestre in range(1, 4):
            # ... (lógica interna do loop e chamada da API inalterada) ...
            url = f'{base_url}?an_exercicio={ano_referencia}&id_ente={ente_ibge}&nr_periodo={trimestre}&in_periodicidade=Q&co_tipo_demonstrativo=RGF&co_poder=E'
            dados_brutos = chama_api(url)
            
            if dados_brutos and dados_brutos.get("items"):
                content = dados_brutos.get("items", [])
                dados_periodo = pd.DataFrame(content)
                dados_consolidados_ano = pd.concat([dados_consolidados_ano, dados_periodo], ignore_index=True)
                print(f"  Trimestre {trimestre}: Recebidos {len(dados_periodo)} registros.")
            else:
                print(f"  Trimestre {trimestre}: Nenhum dado retornado.")
    
    if dados_consolidados_ano.empty:
        print("Atenção: Nenhum dado de RGF foi consolidado para os filtros aplicados.")
    
    return dados_consolidados_ano