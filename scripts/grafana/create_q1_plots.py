import sys
import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import time
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

if len(sys.argv) != 2:
    print("❗️Uso: python3 create_q1_plots.py [datasource.csv]")
    sys.exit(1)

ds = sys.argv[1]

# Configurazione
GRAFANA_URL = "http://localhost:3000"
USERNAME = "admin"
PASSWORD = "admin"
CSV_PATH = f"/var/lib/grafana/csv/{ds}"
PNG_OUTPUT_PATH = "./Results/images"

def save_dashboard():
    try:
        wait = WebDriverWait(driver, 10)

        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid*='Panel menu']"))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[data-testid*='Panel menu item Edit']"))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='data-testid Save dashboard button']"))).click()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='data-testid Save dashboard drawer button']:not([disabled])"))).click()

        print("✅ Dashboard salvata tramite Selenium.")
        
        # 🔄 Ricarica la pagina per assicurarsi che i dati aggiornati siano caricati
        time.sleep(1)
        driver.refresh()
        print("🔁 Dashboard ricaricata.")
        time.sleep(2)  # Dai un attimo a Grafana per ricaricare
        
    except Exception as e:
        print("⚠️ Errore salvataggio dashboard:", e)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
datasource_name = f"CSV_Q1_DS_{timestamp}"
dashboard_uid = f"csvq1dashboard{timestamp}"
dashboard_title = f"Q1 Dashboard CSV {timestamp}"

headers = {
    "Content-Type": "application/json"
}

# 1. Crea il Data Source
payload_ds = {
    "name": datasource_name,
    "type": "marcusolsson-csv-datasource",
    "access": "proxy",
    "url": CSV_PATH,
    "basicAuth": False,
    "isDefault": False,
    "jsonData": {
        "storage": "local",
        "separator": "auto",
        "header": True,
        "skipRows": 0,
        "delimiter": ",",
        "timeField": "year",
        "columns": [
            {"selector": "year", "type": "string"},
            {"selector": "country", "type": "string"},
            {"selector": "carbon-mean", "type": "string"},
            {"selector": "carbon-min", "type": "string"},
            {"selector": "carbon-max", "type": "string"},
            {"selector": "cfe-mean", "type": "string"},
            {"selector": "cfe-min", "type": "string"},
            {"selector": "cfe-max", "type": "string"}
        ]
    }
}

res_ds = requests.post(
    f"{GRAFANA_URL}/api/datasources",
    headers=headers,
    data=json.dumps(payload_ds),
    auth=HTTPBasicAuth(USERNAME, PASSWORD)
)
if res_ds.status_code not in (200, 409):
    print("❌ Errore creazione datasource:", res_ds.text)
    exit(1)

print("✅ Datasource creato.")

# 2. Crea la dashboard con il pannello per Carbon Intensity
dashboard_payload = {
    "dashboard": {
        "id": None,
        "uid": dashboard_uid,
        "title": dashboard_title,
        "timezone": "browser",
        "time": {
            "from": "2019-04-01T13:00:00Z",
            "to": "2026-08-01T12:59:58Z"
        },
        "panels": [
            {
                "id": 1,
                "type": "timeseries",
                "title": "Trend of the Yearly Average Carbon Intensity (gCO₂eq/kWh), 2021–2024: IT vs SE",
                "datasource": datasource_name,
                "targets": [{"refId": "A", "datasource": datasource_name}],
                "fieldConfig": {
                    "defaults": {
                        "custom": {"lineInterpolation": "smooth"},
                        "unit": "none"
                    },
                    "overrides": []
                },
                "gridPos": {"h": 12, "w": 24, "x": 0, "y": 0},
                "options": {
                    "legend": {"displayMode": "list", "placement": "right"},
                    "tooltip": {"mode": "single"}
                },
                "transformations": [
                    {
                        "id": "convertFieldType",
                        "options": {
                            "conversions": [
                                {"targetField": "year", "destinationType": "time"},
                                {"targetField": "carbon-mean", "destinationType": "number"}
                            ]
                        }
                    },
                    {
                        "id": "partitionByValues",
                        "options": {
                            "fields": [
                                "country"
                            ],
                            "keepFields": False,
                            "naming": {
                                "asLabels": True
                            }
                        }
                    },
                    {
                        "id": "renameByRegex",
                        "options": {
                            "regex": "carbon-mean IT",
                            "renamePattern": "Italy Carbon Intensity"
                        }
                    },
                    {
                        "id": "renameByRegex",
                        "options": {
                            "regex": "carbon-mean SE",
                            "renamePattern": "Sweden Carbon Intensity"
                        }
                    }
                ]
            }
        ],
        "schemaVersion": 38,
        "version": 1,
        "refresh": "5s"
    },
    "overwrite": True
}

res_dash = requests.post(
    f"{GRAFANA_URL}/api/dashboards/db",
    headers=headers,
    data=json.dumps(dashboard_payload),
    auth=HTTPBasicAuth(USERNAME, PASSWORD)
)
if res_dash.status_code != 200:
    print("❌ Errore creazione dashboard:", res_dash.text)
    exit(1)

print("✅ Dashboard con pannello Carbon Intensity creata.")

# 3. Login e salvataggio con Selenium
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(options=options)

driver.get(f"{GRAFANA_URL}/login")
time.sleep(2)
# Inserisce username
driver.find_element(By.CSS_SELECTOR, "[data-testid='data-testid Username input field']").send_keys(USERNAME)

# Inserisce password
driver.find_element(By.CSS_SELECTOR, "[data-testid='data-testid Password input field']").send_keys(PASSWORD)

# Clicca il bottone di login
driver.find_element(By.CSS_SELECTOR, "[data-testid='data-testid Login button']").click()
time.sleep(2)

dashboard_url = f"{GRAFANA_URL}/d/{dashboard_uid}/q1-dashboard-csv-{timestamp}?orgId=1"
driver.get(dashboard_url)
time.sleep(2)

save_dashboard()

driver.quit()

# 4. Render PNG per Carbon Intensity
os.makedirs(PNG_OUTPUT_PATH, exist_ok=True)
render_url = f"{GRAFANA_URL}/render/d/{dashboard_uid}/q1-dashboard-csv-{timestamp}?orgId=1&panelId=1&width=1000&height=600"
is_rendered = False
while(is_rendered != True):
    img_carbon = requests.get(render_url, auth=HTTPBasicAuth(USERNAME, PASSWORD), stream=True)
    if img_carbon.status_code == 200:
        with open(os.path.join(PNG_OUTPUT_PATH, "Q1-Carbon.png"), "wb") as f:
            for chunk in img_carbon.iter_content(1024):
                f.write(chunk)
        print("✅ Grafico Carbon Intensity salvato.")
        is_rendered = True
    else:
        print("❌ Errore rendering Carbon Intensity:", img_carbon.status_code)
        print("Riprovando...")

# 5. Aggiorna pannello con dati CFE
dashboard_payload['dashboard']['panels'][0]['title'] = "Trend of the Yearly Average CFE (%), 2021–2024: IT vs SE"
dashboard_payload['dashboard']['panels'][0]['fieldConfig']['defaults']['unit'] = "percent"
dashboard_payload['dashboard']['panels'][0]['targets'][0]['refId'] = "B"
dashboard_payload['dashboard']['panels'][0]['transformations'][0]['options']['conversions'] = [
    {"targetField": "year", "destinationType": "time"},
    {"targetField": "cfe-mean", "destinationType": "number"}
]
dashboard_payload['dashboard']['panels'][0]['transformations'][2]['options']['regex'] = 'cfe-mean IT'
dashboard_payload['dashboard']['panels'][0]['transformations'][2]['options']['renamePattern'] = 'Italy CFE'
dashboard_payload['dashboard']['panels'][0]['transformations'][3]['options']['regex'] = 'cfe-mean SE'
dashboard_payload['dashboard']['panels'][0]['transformations'][3]['options']['renamePattern'] = 'Sweden CFE'

dashboard_payload['dashboard']['version'] += 1  # Incrementa versione

res_update = requests.post(
    f"{GRAFANA_URL}/api/dashboards/db",
    headers=headers,
    data=json.dumps(dashboard_payload),
    auth=HTTPBasicAuth(USERNAME, PASSWORD)
)
if res_update.status_code != 200:
    print("❌ Errore aggiornamento pannello a CFE:", res_update.text)
    exit(1)

print("✅ Pannello aggiornato per CFE.")

# 6. Salva di nuovo la dashboard con Selenium
driver = webdriver.Chrome(options=options)
driver.get(f"{GRAFANA_URL}/login")
time.sleep(2)
# Inserisce username
driver.find_element(By.CSS_SELECTOR, "[data-testid='data-testid Username input field']").send_keys(USERNAME)

# Inserisce password
driver.find_element(By.CSS_SELECTOR, "[data-testid='data-testid Password input field']").send_keys(PASSWORD)

# Clicca il bottone di login
driver.find_element(By.CSS_SELECTOR, "[data-testid='data-testid Login button']").click()
time.sleep(2)
driver.get(dashboard_url)
time.sleep(2)

save_dashboard()

driver.quit()

# 7. Render PNG per CFE
is_rendered = False
while(is_rendered != True):
    img_cfe = requests.get(render_url, auth=HTTPBasicAuth(USERNAME, PASSWORD), stream=True)
    if img_cfe.status_code == 200:
        with open(os.path.join(PNG_OUTPUT_PATH, "Q1-CFE.png"), "wb") as f:
            for chunk in img_cfe.iter_content(1024):
                f.write(chunk)
        print("✅ Grafico CFE salvato.")
        is_rendered = True
    else:
        print("❌ Errore rendering CFE:", img_cfe.status_code)
        print("Riprovando...")

