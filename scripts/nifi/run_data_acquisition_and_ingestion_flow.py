import requests
import time

NIFI_API = "http://localhost:8080/nifi-api"
TEMPLATE_FILE_PATH = "./nifi/templates/data_acquisition_and_ingestion_flow.xml"
TEMPLATE_NAME = "data_acquisition_and_ingestion_flow"

HEADERS = {"Content-Type": "application/json"}

def get_root_process_group_id():
    r = requests.get(f"{NIFI_API}/flow/process-groups/root")
    r.raise_for_status()
    return r.json()["processGroupFlow"]["id"]

def get_controller_services(pg_id):
    r = requests.get(f"{NIFI_API}/process-groups/{pg_id}/controller-services")
    r.raise_for_status()
    return r.json()["controllerServices"]

def update_controller_service_state(service, state):
    service_id = service["id"]
    version = service["revision"]["version"]
    payload = {
        "revision": {"version": version},
        "component": {
            "id": service_id,
            "state": state  # "ENABLED" or "DISABLED"
        }
    }
    r = requests.put(f"{NIFI_API}/controller-services/{service_id}/run-status", json=payload)
    r.raise_for_status()

def delete_controller_service(service):
    service_id = service["id"]
    version = service["revision"]["version"]
    r = requests.delete(f"{NIFI_API}/controller-services/{service_id}?version={version}")
    r.raise_for_status()

def get_processors(pg_id):
    r = requests.get(f"{NIFI_API}/process-groups/{pg_id}/processors")
    r.raise_for_status()
    return r.json()["processors"]

def stop_processor(processor):
    processor_id = processor["id"]
    version = processor["revision"]["version"]
    payload = {
        "revision": {"version": version},
        "state": "STOPPED"
    }
    r = requests.put(f"{NIFI_API}/processors/{processor_id}/run-status", json=payload)
    r.raise_for_status()

def start_processor(processor):
    processor_id = processor["id"]
    version = processor["revision"]["version"]
    payload = {
        "revision": {"version": version},
        "state": "RUNNING"
    }
    r = requests.put(f"{NIFI_API}/processors/{processor_id}/run-status", json=payload)
    r.raise_for_status()

def get_connections(pg_id):
    r = requests.get(f"{NIFI_API}/process-groups/{pg_id}/connections")
    r.raise_for_status()
    return r.json()["connections"]

def delete_connection(connection):
    connection_id = connection["id"]
    version = connection["revision"]["version"]
    r = requests.delete(f"{NIFI_API}/connections/{connection_id}?version={version}")
    r.raise_for_status()

def delete_processor(processor):
    processor_id = processor["id"]
    version = processor["revision"]["version"]
    r = requests.delete(f"{NIFI_API}/processors/{processor_id}?version={version}")
    r.raise_for_status()

def get_templates():
    r = requests.get(f"{NIFI_API}/flow/templates")
    r.raise_for_status()
    return r.json()["templates"]

def delete_template(template):
    template_id = template["id"]
    r = requests.delete(f"{NIFI_API}/templates/{template_id}")
    r.raise_for_status()

def upload_template(pg_id):
    with open(TEMPLATE_FILE_PATH, "rb") as f:
        files = {'template': (TEMPLATE_FILE_PATH, f, 'application/xml')}
        r = requests.post(f"{NIFI_API}/process-groups/{pg_id}/templates/upload", files=files)
        r.raise_for_status()
        return r.json()["template"]["id"]

def instantiate_template(pg_id, template_id):
    payload = {
        "templateId": template_id,
        "originX": 0.0,
        "originY": 0.0
    }
    r = requests.post(f"{NIFI_API}/process-groups/{pg_id}/template-instance", json=payload)
    r.raise_for_status()
    return r.json()["flow"]["processGroups"]

def enable_controller_services(pg_id):
    services = get_controller_services(pg_id)
    for svc in services:
        if svc["component"]["state"] != "ENABLED":
            update_controller_service_state(svc, "ENABLED")

def start_all_processors(pg_id):
    processors = get_processors(pg_id)
    for p in processors:
        start_processor(p)

def clean_canvas(pg_id):
    # 1. Disable and delete controller services
    print("Disabling and deleting controller services...")
    services = get_controller_services(pg_id)
    for svc in services:
        if svc["component"]["state"] == "ENABLED":
            update_controller_service_state(svc, "DISABLED")
    # Wait a moment for disable to take effect
    time.sleep(2)
    for svc in services:
        delete_controller_service(svc)

    # 2. Stop all processors
    print("Stopping all processors...")
    processors = get_processors(pg_id)
    for p in processors:
        if p["component"]["state"] == "RUNNING":
            stop_processor(p)
    time.sleep(2)

    # 3. Delete all connections (queues)
    print("Deleting all connections (queues)...")
    connections = get_connections(pg_id)
    for c in connections:
        delete_connection(c)
    time.sleep(2)

    # 4. Delete all processors
    print("Deleting all processors...")
    processors = get_processors(pg_id)
    for p in processors:
        delete_processor(p)

def main():
    root_pg_id = get_root_process_group_id()
    print(f"Root process group id: {root_pg_id}")

    # Clean canvas
    clean_canvas(root_pg_id)

    # Manage template
    print("Checking existing templates...")
    templates = get_templates()
    existing_template = next((t for t in templates if t["template"]["name"] == TEMPLATE_NAME), None)
    if existing_template:
        print(f"Deleting existing template {TEMPLATE_NAME}...")
        delete_template(existing_template["template"])

    print("Uploading template...")
    template_id = upload_template(root_pg_id)

    print(f"Instantiating template {template_id}...")
    instantiate_template(root_pg_id, template_id)

    print("Enabling controller services...")
    enable_controller_services(root_pg_id)

    print("Starting all processors...")
    start_all_processors(root_pg_id)

    print("Done!")

if __name__ == "__main__":
    main()

