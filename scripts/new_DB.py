import pandas as pd

# Carregar o CSV (separador ; )
df = pd.read_csv("dataset/data/historico.csv", sep=";", encoding="utf-8")

# Manter apenas as colunas necessárias
df = df[['NUMERO_MANIFESTACAO', 'DATA_ACOMPANHAMENTO', 'SITUACAO_ACOMPANHAMENTO']]

# Renomear para facilitar o código
df = df.rename(columns={
    'NUMERO_MANIFESTACAO': 'NUM_MANIFESTACAO',
    'DATA_ACOMPANHAMENTO': 'DATA_EVENTO'
})

# Converter datas
df['DATA_EVENTO'] = pd.to_datetime(df['DATA_EVENTO'], errors='coerce', dayfirst=True)

# Função para calcular os tempos por manifestação
def calcular_tempos(subdf):
    subdf = subdf.sort_values('DATA_EVENTO')

    resultado = {
        'NUM_MANIFESTACAO': subdf['NUM_MANIFESTACAO'].iloc[0],
        'DT_CADASTRADA': pd.NaT,
        'DT_ENCAMINHADA_INTERMEDIARIO': pd.NaT,
        'DT_ENCAMINHADA_ULTIMO': pd.NaT,
        'DT_RESPOSTA': pd.NaT,
        'TEMPO_TOTAL': pd.NA,
        'TEMPO_INTERMEDIARIO': pd.NA,
        'TEMPO_ULTIMO': pd.NA
    }

    # CADASTRADA
    cad = subdf[subdf['SITUACAO_ACOMPANHAMENTO'] == 'CADASTRADA']['DATA_EVENTO']
    if not cad.empty:
        resultado['DT_CADASTRADA'] = cad.iloc[0]

    # Data resposta final (mais recente de RESPOSTA DEFINITIVA, CONCLUIDA, FECHADA)
    finais = subdf[subdf['SITUACAO_ACOMPANHAMENTO'].isin(['RESPOSTA DEFINITIVA', 'CONCLUIDA', 'FECHADA'])]
    if not finais.empty:
        resultado['DT_RESPOSTA'] = finais['DATA_EVENTO'].max()

    # Intermediário (primeira ENCAMINHADA ou ENCAMINHADA PARA PONTO DE RESPOSTA após CADASTRADA)
    if pd.notna(resultado['DT_CADASTRADA']):
        inter = subdf[
            (subdf['DATA_EVENTO'] > resultado['DT_CADASTRADA']) &
            (subdf['SITUACAO_ACOMPANHAMENTO'].isin(['ENCAMINHADA', 'ENCAMINHADA PARA PONTO DE RESPOSTA']))
        ]
        if not inter.empty:
            resultado['DT_ENCAMINHADA_INTERMEDIARIO'] = inter['DATA_EVENTO'].iloc[0]

    # Último agente (mais recente ATRIBUIDA / ENCAMINHADA / ENCAMINHADA PARA PONTO DE RESPOSTA)
    ult = subdf[subdf['SITUACAO_ACOMPANHAMENTO'].isin(['ATRIBUIDA', 'ENCAMINHADA', 'ENCAMINHADA PARA PONTO DE RESPOSTA'])]
    if not ult.empty:
        resultado['DT_ENCAMINHADA_ULTIMO'] = ult['DATA_EVENTO'].max()

    # Calcular tempos em dias
    if pd.notna(resultado['DT_CADASTRADA']) and pd.notna(resultado['DT_RESPOSTA']):
        resultado['TEMPO_TOTAL'] = (resultado['DT_RESPOSTA'] - resultado['DT_CADASTRADA']).days

    if pd.notna(resultado['DT_ENCAMINHADA_INTERMEDIARIO']) and pd.notna(resultado['DT_RESPOSTA']):
        resultado['TEMPO_INTERMEDIARIO'] = (resultado['DT_RESPOSTA'] - resultado['DT_ENCAMINHADA_INTERMEDIARIO']).days

    if pd.notna(resultado['DT_ENCAMINHADA_ULTIMO']) and pd.notna(resultado['DT_RESPOSTA']):
        resultado['TEMPO_ULTIMO'] = (resultado['DT_RESPOSTA'] - resultado['DT_ENCAMINHADA_ULTIMO']).days

    return pd.Series(resultado)

# Aplicar a cada manifestação
NEW_DB = df.groupby('NUM_MANIFESTACAO').apply(calcular_tempos).reset_index(drop=True)

# Salvar resultado como CSV (também em ;)
NEW_DB.to_csv("NEW_DB.csv", sep=";", index=False, encoding="utf-8")

print(NEW_DB)


