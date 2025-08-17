import pandas as pd
# --- PONTO CHAVE: Importa nossa função de leitura inteligente ---
from include.functions import ler_arquivo_parquet

def transformar_rgf_silver_para_gold(
    caminho_anexo_01_silver: str,
    aws_credentials=None
) -> pd.DataFrame:
    """
    Lê o arquivo do Anexo 01 do RGF da camada Silver e o transforma em uma
    tabela Gold com indicadores chave de Receita e Despesa com Pessoal.
    """
    print("--- INICIANDO TRANSFORMAÇÃO SILVER -> GOLD (RGF) ---")
    
    # --- PONTO CHAVE: Usa a função reutilizável ---
    # Esta função já sabe se o caminho é local ou S3 e como usar
    # as credenciais (ou não) de acordo.
    df_silver = ler_arquivo_parquet(
        caminho_do_arquivo=caminho_anexo_01_silver,
        aws_credentials=aws_credentials
    )

    if df_silver.empty:
        print("DataFrame do Anexo 01 Silver está vazio. Pulando a transformação Gold.")
        return pd.DataFrame()

    # ... (O restante da lógica de filtragem e seleção de colunas continua o mesmo) ...
    
    contas_desejadas = [
        "RECEITA CORRENTE LIQUIDA - RCL (IV)",
        "DESPESA TOTAL COM PESSOAL - DTP (VI) = (IIIa + IIIb)"
    ]
    colunas_desejadas = ["exercicio", "periodo", "conta", "cod_ibge", "valor"]

    df_filtrado = df_silver[df_silver['conta'].isin(contas_desejadas)]
    
    if 'valor' in df_filtrado.columns:
        df_filtrado['valor'] = pd.to_numeric(df_filtrado['valor'], errors='coerce')

    df_gold = df_filtrado[colunas_desejadas]
    
    print(f"--- TRANSFORMAÇÃO GOLD CONCLUÍDA: {len(df_gold)} registros preparados. ---")
    return df_gold

# --- NOVA FUNÇÃO PARA O RREO GOLD ---
def transformar_rreo_silver_para_gold(
    caminho_anexo_01_silver: str,
    aws_credentials=None
) -> pd.DataFrame:
    """
    Lê o arquivo do Anexo 01 do RREO da camada Silver e o transforma em uma
    tabela Gold com indicadores de Receitas e Despesas.
    """
    print("--- INICIANDO TRANSFORMAÇÃO SILVER -> GOLD (RREO) ---")
    
    # 1. Ler o arquivo da camada Silver
    df_silver = ler_arquivo_parquet(
        caminho_do_arquivo=caminho_anexo_01_silver,
        aws_credentials=aws_credentials
    )

    if df_silver.empty:
        print("DataFrame do Anexo 01 Silver (RREO) está vazio. Pulando a transformação Gold.")
        return pd.DataFrame()

    # 2. Definir e aplicar as regras de negócio
    print("Aplicando filtros para Receitas Correntes e Despesas Liquidadas...")
    
    # Condição A: Receitas Correntes no bimestre
    condicao_receitas = (df_silver['coluna'] == 'Até o Bimestre (c)') & \
                        (df_silver['conta'] == 'RECEITAS CORRENTES')
    
    # Condição B: Despesas liquidadas no bimestre
    condicao_despesas = (df_silver['coluna'] == 'DESPESAS LIQUIDADAS ATÉ O BIMESTRE (h)') & \
                        (df_silver['conta'] == 'DESPESAS CORRENTES')

    # Combina as duas condições com um "OU"
    df_filtrado = df_silver[condicao_receitas | condicao_despesas]

    # 3. Selecionar as colunas finais
    colunas_desejadas = [
        "exercicio",
        "periodo",
        "cod_ibge",
        "coluna",
        "conta",
        "valor"
    ]
    
    # Garante que as colunas existem antes de tentar selecioná-las
    colunas_existentes = [col for col in colunas_desejadas if col in df_filtrado.columns]
    df_gold = df_filtrado[colunas_existentes].copy() # Usar .copy() para evitar warnings
    
    if 'valor' in df_gold.columns:
        df_gold['valor'] = pd.to_numeric(df_gold['valor'], errors='coerce')

    print(f"--- TRANSFORMAÇÃO GOLD (RREO) CONCLUÍDA: {len(df_gold)} registros preparados. ---")
    return df_gold