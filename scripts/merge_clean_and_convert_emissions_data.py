import pandas as pd
from pathlib import Path

# Directory dove hai salvato i CSV originali
data_dir = Path(__file__).resolve().parent.parent / 'data/raw'

# Directory dove salvare i file uniti, puliti e convertiti in Parquet
processed_dir = Path(__file__).resolve().parent.parent / 'data/processed'
processed_dir.mkdir(parents=True, exist_ok=True)  # Crea la cartella se non esiste

# ---- FUNZIONE DI CARICAMENTO E PULIZIA ---- #
def load_and_clean(file_path):
    df = pd.read_csv(file_path)
    df = df[[
        "Datetime (UTC)",
        "Zone id",
        "Carbon intensity gCO₂eq/kWh (direct)",
        "Carbon-free energy percentage (CFE%)"
    ]]
    df.columns = ["datetime", "country_code", "carbon_intensity_direct", "cfe_percent"]
    return df

# ---- Uniamo e puliamo ITALIA ---- #
italy_files = sorted(data_dir.glob("IT_*.csv"))
italy_df = pd.concat([load_and_clean(f) for f in italy_files])
italy_clean_path = processed_dir / "italy_2021_2024_clean.csv"
italy_df.to_csv(italy_clean_path, index=False)
print(f"File unito e pulito: {italy_clean_path}")

# ---- Uniamo e puliamo SVEZIA ---- #
sweden_files = sorted(data_dir.glob("SE_*.csv"))
sweden_df = pd.concat([load_and_clean(f) for f in sweden_files])
sweden_clean_path = processed_dir / "sweden_2021_2024_clean.csv"
sweden_df.to_csv(sweden_clean_path, index=False)
print(f"File unito e pulito: {sweden_clean_path}")

# Funzione per caricare e convertire in Parquet
def load_and_convert(file_path, output_path):
    # Carica il CSV
    df = pd.read_csv(file_path)
    
    # Converte in Parquet
    df.to_parquet(output_path, index=False, engine='pyarrow')
    print(f"File convertito: {output_path}")

# ---- Convertiamo ITALIA ---- #
italy_parquet_path = processed_dir / "italy_2021_2024_clean.parquet"
load_and_convert(italy_clean_path, italy_parquet_path)

# ---- SConvertiamo VEZIA ---- #
sweden_parquet_path = processed_dir / "sweden_2021_2024_clean.parquet"
load_and_convert(sweden_clean_path, sweden_parquet_path)

print("✅ CSV uniti, puliti e convertiti in Parquet.")

