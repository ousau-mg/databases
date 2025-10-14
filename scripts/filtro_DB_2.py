
## DB_2
import pandas as pd
import unicodedata
from IPython.display import display

def padronizar_colunas(df):
    df.columns = (
        df.columns.str.strip()
                  .str.upper()
                  .str.normalize("NFKD")
                  .str.encode("ascii", errors="ignore")
                  .str.decode("utf-8")
    )
    return df

def normalize_text(x):
    if pd.isna(x): return ""
    s = str(x)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode()
    return s.upper().strip()

# --- Ler DB_1 ---
DB_1 = pd.read_excel("/content/DB_1.xlsx")
DB_1["PROTOCOLO"] = DB_1["PROTOCOLO"].astype(str).str.strip()
protocolos_set = set(DB_1["PROTOCOLO"])

# --- Caminho ---
arquivo_excel = "/content/Banco de Dados - Novo OuvidorSUS - Histórico de Situação e Status.xlsx"
saida_csv = "/content/DB_2.csv"

# --- Ler as abas inteiras (sem chunksize) ---
df0 = pd.read_excel(arquivo_excel, sheet_name="Sheet0", engine="openpyxl")
df1 = pd.read_excel(arquivo_excel, sheet_name="Sheet1", engine="openpyxl")

# Concatenar
df_hist = pd.concat([df0, df1], ignore_index=True)
df_hist = padronizar_colunas(df_hist)
df_hist["NUMERO_MANIFESTACAO"] = df_hist["NUMERO_MANIFESTACAO"].astype(str).str.strip()
df_hist["SITUACAO_ACOMPANHAMENTO"] = df_hist["SITUACAO_ACOMPANHAMENTO"].apply(normalize_text)
df_hist["DATA_ACOMPANHAMENTO"] = pd.to_datetime(df_hist["DATA_ACOMPANHAMENTO"], errors="coerce", dayfirst=True)

# --- Filtrar protocolos de DB_1 ---
df_hist = df_hist[df_hist["NUMERO_MANIFESTACAO"].isin(protocolos_set)]

# --- Seleção final ---
DB_2 = df_hist[[
    "NUMERO_MANIFESTACAO",
    "DATA_ACOMPANHAMENTO",
    "SITUACAO_ACOMPANHAMENTO",
    "NOME_OUVIDORIA_DESTINO",
    "MUNICIPIO_OUVIDORIA_DESTINO"
]].copy()

# --- Salvar ---
DB_2.to_csv(saida_csv, sep=";", encoding="latin1", index=False)

print("✅ DB_2 salvo em", saida_csv)
print("Shape:", DB_2.shape)
display(DB_2.head())