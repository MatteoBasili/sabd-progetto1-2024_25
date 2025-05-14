import pandas as pd
from pathlib import Path

# Directory dove hai salvato i CSV originali
data_dir = Path(__file__).resolve().parent.parent / 'data/raw'

# Directory dove salvare i file uniti e puliti
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

# ---- ITALIA ---- #
italy_files = sorted(data_dir.glob("IT_*.csv"))
italy_df = pd.concat([load_and_clean(f) for f in italy_files])
italy_df.to_csv(processed_dir / "italy_2021_2024_clean.csv", index=False)

# ---- SVEZIA ---- #
sweden_files = sorted(data_dir.glob("SE_*.csv"))
sweden_df = pd.concat([load_and_clean(f) for f in sweden_files])
sweden_df.to_csv(processed_dir / "sweden_2021_2024_clean.csv", index=False)

print("✅ CSV uniti e puliti.")

