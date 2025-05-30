import requests
import os
import time

# Config
NIFI_API_URL = "http://localhost:8080/nifi-api"
TEMPLATE_PATH = "./nifi/templates/data_acquisition_and_ingestion_flow.xml"

def upload_template(template_path):
    print(f"⬆️ Carico il template da: {template_path}")
    with open(template_path, 'rb') as f:
        response = requests.post(
            f"{NIFI_API_URL}/process-groups/root/templates/upload",
            files={'template': f}
        )

    if response.status_code == 201:
        try:
            return response.json()['template']['id']
        except Exception:
            print("⚠️ Nessuna risposta JSON. Template caricato comunque.")
            return find_template_id_by_name(template_path)

    elif response.status_code == 409:
        print("⚠️ Template già presente. Lo recupero...")
        return find_template_id_by_name(template_path)

    else:
        print(response.text)
        raise RuntimeError("❌ Upload fallito.")

def find_template_id_by_name(template_path):
    template_name = os.path.basename(template_path).replace(".xml", "")
    response = requests.get(f"{NIFI_API_URL}/flow/templates")
    for t in response.json().get("templates", []):
        if t["template"]["name"] == template_name:
            return t["template"]["id"]
    raise RuntimeError("❌ Template non trovato.")

def instantiate_template(template_id, x=0, y=0):
    print(f"📦 Istanzio il template: {template_id}")
    url = f"{NIFI_API_URL}/process-groups/root/template-instance"
    payload = {"templateId": template_id, "originX": x, "originY": y}
    response = requests.post(url, json=payload)
    response.raise_for_status()

    print("🔍 Recupero il nuovo process group...")
    time.sleep(1)  # Attendi per sicurezza
    groups = requests.get(f"{NIFI_API_URL}/flow/process-groups/root").json()
    for pg in groups['processGroupFlow']['flow']['processGroups']:
        if pg['component']['name'].startswith("data_acquisition"):
            return pg['component']['id']
    raise RuntimeError("❌ Process Group non trovato.")

def enable_controller_services(pg_id):
    print(f"⚙️ Abilitazione controller services per process group: {pg_id}")
    url = f"{NIFI_API_URL}/flow/process-groups/{pg_id}/controller-services"
    services = requests.get(url).json()["controllerServices"]

    for svc in services:
        svc_id = svc['component']['id']
        state = svc['component']['state']
        name = svc['component']['name']
        if state != "ENABLED":
            print(f"🔄 Abilitazione: {name}")
            svc_url = f"{NIFI_API_URL}/controller-services/{svc_id}/run-status"
            body = {
                "revision": svc["revision"],
                "state": "ENABLED"
            }
            r = requests.put(svc_url, json=body)
            if r.status_code not in (200, 202):
                print(f"❌ Errore abilitando {name}: {r.text}")
            else:
                print(f"✅ {name} abilitato.")

def list_processors(pg_id):
    url = f"{NIFI_API_URL}/process-groups/{pg_id}/processors"
    response = requests.get(url)
    response.raise_for_status()
    processors = response.json()["processors"]
    return [(p["id"], p["component"]["name"]) for p in processors]

def start_processor(proc_id):
    url = f"{NIFI_API_URL}/processors/{proc_id}/run-status"
    print(f"▶️ Avvio processore: {proc_id}")
    proc_info = requests.get(f"{NIFI_API_URL}/processors/{proc_id}").json()
    body = {
        "revision": proc_info["revision"],
        "state": "RUNNING"
    }
    r = requests.put(url, json=body)
    if r.status_code in (200, 202):
        print("✅ Avviato.")
    else:
        print(f"❌ Errore: {r.text}")

def main():
    template_id = upload_template(TEMPLATE_PATH)
    pg_id = instantiate_template(template_id)
    enable_controller_services(pg_id)

    print("\n📋 Lista dei processori disponibili:")
    processors = list_processors(pg_id)
    for idx, (_, name) in enumerate(processors):
        print(f"{idx + 1}. {name}")

    while True:
        choice = input("\nInserisci il numero del processore da avviare (0 per uscire): ")
        if not choice.isdigit():
            print("❗ Inserisci un numero valido.")
            continue

        choice = int(choice)
        if choice == 0:
            break
        elif 1 <= choice <= len(processors):
            proc_id, name = processors[choice - 1]
            print(f"➡️ Avvio {name}...")
            start_processor(proc_id)
        else:
            print("❗ Numero non valido.")

if __name__ == "__main__":
    main()

