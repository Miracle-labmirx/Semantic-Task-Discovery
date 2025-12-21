import requests
import pandas as pd
import openpyxl

api_endpoints = ["https://project-sato1.lasige.di.fc.ul.pt/api", "https://project-sato2.lasige.di.fc.ul.pt/api"]
headers = {"x-api-key": "sato_1oyQx-XWRQ1tq7VjLxIw_2NNWwLct7onsRGbQNNYTzs"}

def sato_query(query):
    for i in range(2):
        payload = requests.get(api_endpoints[i] + query, headers=headers, verify=True)
        if(payload.status_code == 200): 
            return payload
    else:
        return None 


def list_entities(entity_type, idPattern=None, q=None, options=None):
    endpoint = type_to_entrypoint(entity_type=entity_type)
    query = f"/{endpoint}?"

    if (idPattern is not None):
        query += f"idPattern=.*{idPattern}.*"
    if (q is not None):
        query += f"&q={q}"
    if (options is not None):
        query += f"&options={options}"

    offset = 0
    all_entities = []
    while (True):
        entity = sato_query(query + f"&offset={offset}")
        if (entity.status_code == 200):
            entities = entity.json()
            if (len(entities) == 0):
                return all_entities
            else:
                all_entities += entities
            offset += 100
    else:
        raise Exception(f"FAILED to get entity info: {query}")

def type_to_entrypoint(entity_type):
    entity_to_endpoint = {
        "Device": "devices",
        "Building": "buildings",
        "Zone": "zones",
        "Space": "spaces",
        "Site": "sites",
        "External": "external",
        "Task": "tasks"
    }
    return entity_to_endpoint[entity_type]


def create_node(tx, query, params):
    tx.run(query, **params)

def clear_neo4j_database():
    query = "MATCH (n) DETACH DELETE n"
    with driver.session() as session:
        session.run(query)
        print("All nodes and relationships deleted.")

def fetch_and_save_all_entities(filename="sato_entities.xlsx"):
    entity_types = ["Building", "Zone", "Space", "Device"]
    # all_rows = []

    for entity_type in entity_types:
        print(f"Fetching {entity_type}...")
        entities = list_entities(entity_type)
        rows = []
        for e in entities:
            flat = {"type": entity_type}
            flat.update(e)
            # all_rows.append(flat)
            rows.append(flat)
     
        df = pd.DataFrame(rows)
        filename = f"sato_{entity_type.lower()}.xlsx"
        df.to_excel(filename, index=False, engine="openpyxl")
        print(f"Saved {len(df)} rows to {filename}")

# Run the export
fetch_and_save_all_entities()