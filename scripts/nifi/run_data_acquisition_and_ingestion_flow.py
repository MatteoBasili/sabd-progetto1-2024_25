import requests
import time
import uuid
import xml.etree.ElementTree as ET

NIFI_API = "http://localhost:8080/nifi-api"
PROCESS_GROUP_ID = "root"  # root canvas
WAIT_SECONDS = 2

def get_all_processor_ids():
    url = f"{NIFI_API}/flow/process-groups/{PROCESS_GROUP_ID}"
    response = requests.get(url)
    processors = response.json()['processGroupFlow']['flow']['processors']
    return [(p['id'], p['component']['state']) for p in processors]

def stop_all_processors():
    print("Stopping all existing processors...")
    processors = get_all_processor_ids()
    for pid, state in processors:
        if state != "STOPPED":
            url = f"{NIFI_API}/processors/{pid}/run-status"
            body = {
                "revision": get_revision(pid, "processors"),
                "state": "STOPPED",
                "disconnectedNodeAcknowledged": False
            }
            requests.put(url, json=body)
    time.sleep(WAIT_SECONDS)

def get_all_controller_services():
    url = f"{NIFI_API}/flow/process-groups/{PROCESS_GROUP_ID}/controller-services"
    response = requests.get(url)
    services = response.json()['controllerServices']
    return [(s['id'], s['component']['state']) for s in services]

def disable_all_controller_services():
    print("Disabling all existing controller services...")
    services = get_all_controller_services()
    for sid, state in services:
        if state != "DISABLED":
            url = f"{NIFI_API}/controller-services/{sid}/run-status"
            body = {
                "revision": get_revision(sid, "controller-services"),
                "state": "DISABLED"
            }
            requests.put(url, json=body)
    time.sleep(WAIT_SECONDS)

def get_all_connections():
    url = f"{NIFI_API}/flow/process-groups/{PROCESS_GROUP_ID}"
    response = requests.get(url)
    connections = response.json()['processGroupFlow']['flow']['connections']
    return [conn['id'] for conn in connections]

def drop_all_queues():
    print("Dropping all existing queues...")
    connections = get_all_connections()
    for cid in connections:
        url = f"{NIFI_API}/flowfile-queues/{cid}/drop-requests"
        body = {"connectionId": cid}
        resp = requests.post(url, json=body)
        drop_request = resp.json()
        drop_id = drop_request['dropRequest']['id']
        # Wait until drop is finished
        while True:
            check_url = f"{NIFI_API}/flowfile-queues/{cid}/drop-requests/{drop_id}"
            check = requests.get(check_url).json()
            if check['dropRequest']['finished']:
                break
            time.sleep(WAIT_SECONDS)

def delete_all_connections():
    print("Deleting all existing connections...")
    for cid in get_all_connections():
        rev = get_revision(cid, "connections")
        url = f"{NIFI_API}/connections/{cid}?version={rev['version']}&clientId={rev['clientId']}"
        requests.delete(url)

def delete_all_processors():
    print("Deleting all existing processors...")
    for pid, _ in get_all_processor_ids():
        rev = get_revision(pid, "processors")
        url = f"{NIFI_API}/processors/{pid}?version={rev['version']}&clientId={rev['clientId']}"
        requests.delete(url)

def delete_all_controller_services():
    print("Deleting all existing controller services...")
    for sid, _ in get_all_controller_services():
        rev = get_revision(sid, "controller-services")
        url = f"{NIFI_API}/controller-services/{sid}?version={rev['version']}&clientId={rev['clientId']}"
        requests.delete(url)

def get_revision(component_id, component_type):
    url = f"{NIFI_API}/{component_type}/{component_id}"
    response = requests.get(url)
    revision = response.json()['revision']
    # Assicura clientId se non presente
    if 'clientId' not in revision:
        revision['clientId'] = str(uuid.uuid4())
    return revision

def upload_template(template_path):
    print("Uploading template...")
    with open(template_path, 'rb') as f:
        files = {'template': (template_path, f, 'application/xml')}
        url = f"{NIFI_API}/process-groups/{PROCESS_GROUP_ID}/templates/upload"
        response = requests.post(url, files=files)

        if not response.ok:
            print(f"❌ Template upload failed with status code {response.status_code}")
            print("Response text:", response.text)
            response.raise_for_status()

        # Parse XML response to extract template ID
        try:
            root = ET.fromstring(response.text)
            template_id = root.find('.//id').text
            print(f"✅ Template uploaded with ID: {template_id}")
            return template_id
        except Exception as e:
            print("❌ Failed to parse template upload response as XML.")
            print("Raw response:", response.text)
            raise e
        
def instantiate_template(template_id, x=0.0, y=0.0):
    print("Instantiating template on canvas...")
    url = f"{NIFI_API}/process-groups/{PROCESS_GROUP_ID}/template-instance"
    body = {
        "templateId": template_id,
        "originX": x,
        "originY": y,
        "disconnectedNodeAcknowledged": False
    }
    response = requests.post(url, json=body)
    response.raise_for_status()
    print("✅ Template instantiated successfully.")

def enable_all_controller_services():
    print("Enabling controller services...")
    services = get_all_controller_services()
    for sid, state in services:
        if state != "ENABLED":
            url = f"{NIFI_API}/controller-services/{sid}/run-status"
            body = {
                "revision": get_revision(sid, "controller-services"),
                "state": "ENABLED"
            }
            requests.put(url, json=body)
    time.sleep(WAIT_SECONDS)

def delete_template_by_name(template_name):
    print(f"Checking if template '{template_name}' already exists...")
    url = f"{NIFI_API}/flow/templates"
    response = requests.get(url)
    response.raise_for_status()
    templates = response.json().get("templates", [])
    for tmpl in templates:
        if tmpl["template"]["name"] == template_name:
            tid = tmpl["template"]["id"]
            del_url = f"{NIFI_API}/templates/{tid}"
            del_resp = requests.delete(del_url)
            del_resp.raise_for_status()
            print(f"✅ Deleted existing template '{template_name}' with ID: {tid}")
            return
    print(f"No existing template named '{template_name}' found.")

def main():
    stop_all_processors()
    disable_all_controller_services()
    drop_all_queues()
    delete_all_connections()
    delete_all_processors()
    delete_all_controller_services()
    
    print("✅ Nifi Canvas cleaned successfully.")
    
    # Upload and instantiate template
    template_path = "./nifi/templates/data_acquisition_and_ingestion_flow.xml"
    template_name = "data_acquisition_and_ingestion_flow"
    
    delete_template_by_name(template_name)
    template_id = upload_template(template_path)
    instantiate_template(template_id)

    # Enable controller services
    enable_all_controller_services()

    print("✅ Template deployed and controller services enabled.")

def get_processor_id_by_name(name):
    url = f"{NIFI_API}/flow/process-groups/{PROCESS_GROUP_ID}"
    response = requests.get(url)
    processors = response.json()['processGroupFlow']['flow']['processors']
    for p in processors:
        if p['component']['name'] == name:
            return p['component']['id']
    raise ValueError(f"Processor '{name}' not found.")

def run_processor_once(pid):
    url = f"{NIFI_API}/processors/{pid}/run-status"
    body = {
        "revision": get_revision(pid, "processors"),
        "state": "RUN_ONCE",
        "disconnectedNodeAcknowledged": False
    }
    requests.put(url, json=body)

def run_processor(pid):
    url = f"{NIFI_API}/processors/{pid}/run-status"
    body = {
        "revision": get_revision(pid, "processors"),
        "state": "RUNNING",
        "disconnectedNodeAcknowledged": False
    }
    requests.put(url, json=body)

def stop_processor(pid):
    url = f"{NIFI_API}/processors/{pid}/run-status"
    body = {
        "revision": get_revision(pid, "processors"),
        "state": "STOPPED",
        "disconnectedNodeAcknowledged": False
    }
    requests.put(url, json=body)

def get_all_processor_names():
    url = f"{NIFI_API}/flow/process-groups/{PROCESS_GROUP_ID}"
    response = requests.get(url)
    processors = response.json()['processGroupFlow']['flow']['processors']
    return {p['component']['name']: p['component']['id'] for p in processors}

def simulate_data_flow():
    print("🚀 Running data flow...")

    processors = get_all_processor_names()

    # Step 1: Run Init and GenerateDataURLs once
    run_processor_once(processors["Init"])
    time.sleep(WAIT_SECONDS)
    run_processor_once(processors["GenerateDataURLs"])
    time.sleep(WAIT_SECONDS)

    # Step 2: Run all remaining processors
    print("▶️ Starting processors...")
    to_run = [
        "GetElectricityDataFiles", "CleanData", "RouteOnAttribute",
        "UpdateAttribute_IT", "UpdateAttribute_SE",
        "MergeFiles", "ConvertToParquet", "RenameCsvFile",
        "RenameParquetFile", "PutHDFS"
    ]
    for name in to_run:
        run_processor(processors[name])
        time.sleep(WAIT_SECONDS)

    # Step 3: Monitor until PutHDFS has processed exactly 4 input flowfiles
    print("⏳ Monitoring 'PutHDFS' for completion of exactly 4 input flowfiles...")
    puthdfs_id = processors["PutHDFS"]
    
    while True:
        url = f"{NIFI_API}/flow/processors/{puthdfs_id}/status"
        response = requests.get(url)

        if not response.ok:
            print(f"❌ Failed to get status of processor {puthdfs_id}. HTTP {response.status_code}")
            print("Response body:", response.text)
            break

        try:
            resp = response.json()
            snapshot = resp['processorStatus']['aggregateSnapshot']
            flowfiles_in = snapshot.get('flowFilesIn', 0)
            active_threads = snapshot.get('activeThreadCount', 0)

            print(f"🔄 PutHDFS status → flowFilesIn: {flowfiles_in}, activeThreads: {active_threads}")
        except Exception as e:
            print(f"❌ Error parsing status JSON for processor {puthdfs_id}: {e}")
            print("Raw response:", response.text)
            break

        if flowfiles_in == 4 and active_threads == 0:
            print("✅ PutHDFS has processed exactly 4 flowfiles and is now idle.")
            break

        time.sleep(WAIT_SECONDS)

    # Step 4: Stop all processors and services
    print("🛑 Stopping all processors and disabling controller services...")
    stop_all_processors()
    disable_all_controller_services()
    drop_all_queues()
    print("🏁 Ingestion complete.")

# Esegui tutto in sequenza
if __name__ == "__main__":
    main()
    simulate_data_flow()

