import pandas as pd
from pathlib import Path

# Directory dove hai salvato i CSV puliti
data_dir = Path(__file__).resolve().parent.parent / 'data/processed'

# Funzione per caricare e convertire
def load_and_convert(file_path, output_path):
    # Carica il CSV
    df = pd.read_csv(file_path)
    
    # Converte in Parquet
    df.to_parquet(output_path, index=False, engine='pyarrow')
    print(f"✅ File convertito: {output_path}")

# ---- ITALIA ---- #
italy_clean_path = data_dir / "italy_2021_2024_clean.csv"
italy_parquet_path = data_dir / "italy_2021_2024_clean.parquet"
load_and_convert(italy_clean_path, italy_parquet_path)

# ---- SVEZIA ---- #
sweden_clean_path = data_dir / "sweden_2021_2024_clean.csv"
sweden_parquet_path = data_dir / "sweden_2021_2024_clean.parquet"
load_and_convert(sweden_clean_path, sweden_parquet_path)

