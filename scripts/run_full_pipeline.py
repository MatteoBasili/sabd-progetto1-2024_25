import subprocess
import sys

def run_command(command, capture_output=False):
    print(f"⚙️  Eseguo: {command}")
    result = subprocess.run(command, shell=True, text=True, capture_output=capture_output)
    if result.returncode != 0:
        print(f"❌ Errore nell'esecuzione: {result.stderr}")
        sys.exit(result.returncode)
    return result.stdout if capture_output else None

def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ["q1", "q2", "q3"] or sys.argv[2] not in ["rdd", "df", "sql"]:
        print("❗️Uso: python3 run_full_pipeline.py [q1|q2|q3] [rdd|df|sql]")
        sys.exit(1)

    query = sys.argv[1]
    mode = sys.argv[2]

    print("📥 Avvio Acquisizione e Ingestion dei dati...")
    run_command("python3 ./scripts/nifi/run_data_acquisition_and_ingestion_flow.py")

    print(f"📊 Query scelta: {query.capitalize()}. Metodo selezionato: {mode}. Avvio pipeline...")
    run_command(f"python3 ./scripts/run_{query}_pipeline.py {mode}")
    
    print(f"✅ Query {query.capitalize()} completata.")
    
if __name__ == "__main__":
    main()

