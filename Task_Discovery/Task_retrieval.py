# import requests
# import pandas as pd
# import openpyxl
# # from urllib.parse import quote_plus   # <-- CHANGED: to safely encode the q string

# api_endpoints = ["https://project-sato1.lasige.di.fc.ul.pt/api",
#                  "https://project-sato2.lasige.di.fc.ul.pt/api"]
# headers = {"x-api-key": "sato_1oyQx-XWRQ1tq7VjLxIw_2NNWwLct7onsRGbQNNYTzs"}

# def sato_query(query):
#     for i in range(2):
#         payload = requests.get(api_endpoints[i] + query, headers=headers, verify=True)
#         if payload.status_code == 200:
#             return payload
#     else:
#         return None

# def list_entities(entity_type, idPattern=None, q=None, options=None):
#     endpoint = type_to_entrypoint(entity_type=entity_type)
#     query = f"/{endpoint}?"

#     if idPattern is not None:
#         query += f"idPattern=.*{idPattern}.*"
#     if q is not None:
#         # <-- CHANGED: ensure the NGSI "q" expression is URL-encoded
#         query += f"&q={q}"
#     if options is not None:
#         query += f"&options={options}"

#     offset = 0
#     all_entities = []
#     while True:
#         entity = sato_query(query + f"&offset={offset}")
#         if entity.status_code == 200:
#             entities = entity.json()
#             if len(entities) == 0:
#                 return all_entities
#             all_entities += entities
#             offset += 100
#     else:
#         raise Exception(f"FAILED to get entity info: {query}")

# def type_to_entrypoint(entity_type):
#     entity_to_endpoint = {
#         "Device": "devices",
#         "Building": "buildings",
#         "Zone": "zones",
#         "Space": "spaces",
#         "Site": "sites",
#         "External": "external",
#         "Task": "tasks"
#     }
#     return entity_to_endpoint[entity_type]

# # <-- CHANGED: new focused exporter that only fetches Tasks in categories Assessment or KPI
# def fetch_and_save_generic_tasks(filename="sato_generic_tasks.xlsx"):
#     # Postman equivalent:
#     #   /api/tasks?q=category=="Assessment" or category=="KPI"
#     q_expr = 'category=="Assessment"'
#     tasks = list_entities("Task", q=q_expr, options="full")

#     # Flatten & save
#     rows = []
#     for t in tasks:
#         flat = {"type": "Task"}
#         flat.update(t)
#         rows.append(flat)

#     df = pd.DataFrame(rows)
#     df.to_excel(filename, index=False, engine="openpyxl")
#     print(f"Saved {len(df)} task rows to {filename}")

# # Run the export (only Tasks meeting the two categories)
# fetch_and_save_generic_tasks()


# import requests
# import pandas as pd
# import openpyxl
# from urllib.parse import quote_plus

# api_endpoints = ["https://project-sato1.lasige.di.fc.ul.pt/api",
#                  "https://project-sato2.lasige.di.fc.ul.pt/api"]
# headers = {"x-api-key": "sato_1oyQx-XWRQ1tq7VjLxIw_2NNWwLct7onsRGbQNNYTzs"}

# def sato_query(query):
#     for i in range(2):
#         payload = requests.get(api_endpoints[i] + query, headers=headers, verify=True)
#         if payload.status_code == 200:
#             return payload
#     return None

# def type_to_entrypoint(entity_type):
#     return {
#         "Device": "devices",
#         "Building": "buildings",
#         "Zone": "zones",
#         "Space": "spaces",
#         "Site": "sites",
#         "External": "external",
#         "Task": "tasks"
#     }[entity_type]

# def list_entities(entity_type, idPattern=None, q=None, options=None):
#     endpoint = type_to_entrypoint(entity_type=entity_type)
#     query = f"/{endpoint}?"

#     if idPattern is not None:
#         query += f"idPattern=.*{idPattern}.*"
#     if q is not None:
#         query += f"&q={quote_plus(q)}"   # URL-encode the q expression
#     if options is not None:
#         query += f"&options={options}"

#     offset = 0
#     all_entities = []
#     while True:
#         entity = sato_query(query + f"&offset={offset}")
#         if entity and entity.status_code == 200:
#             batch = entity.json()
#             if not batch:
#                 return all_entities
#             all_entities += batch
#             offset += 100
#         else:
#             raise Exception(f"FAILED to get entity info: {query}")

# # -------- NEW function that loops over q-expressions and keeps your flatten/save block --------
# def fetch_and_save_generic_tasks(filename="sato_generic_tasks.xlsx"):
#     # List of category filters to apply (loop over these)
#     q_exprs = [
#         'category=="Assessment"',
#         'category=="KPI"',
#         'category=="Assessment-Instance"',
#         'category=="KPI-Instance"',
#         'category=="Service-Instance"'
#     ]

#     # Fetch tasks for each q and merge
#     tasks = []
#     for q in q_exprs:
#         tasks.extend(list_entities("Task", q=q, options="full"))

#     # Optionally de-duplicate by 'id' before saving (comment out if not desired)
#     # seen, deduped = set(), []
#     # for t in tasks:
#         # tid = t.get("id")
#         # if tid not in seen:
#             # seen.add(tid)
#             # deduped.append(t)
#     # tasks = deduped

#     # Flatten & save
#     rows = []
#     for t in tasks:
#         flat = {"type": "Task"}
#         flat.update(t)
#         rows.append(flat)

#     df = pd.DataFrame(rows)
#     df.to_excel(filename, index=False, engine="openpyxl")
#     print(f"Saved {len(df)} task rows to {filename}")

# # Run the export (only Tasks meeting the two categories)
# fetch_and_save_generic_tasks()


import requests
import pandas as pd
import openpyxl
from urllib.parse import quote_plus

api_endpoints = ["https://project-sato1.lasige.di.fc.ul.pt/api",
                 "https://project-sato2.lasige.di.fc.ul.pt/api"]
headers = {"x-api-key": "sato_1oyQx-XWRQ1tq7VjLxIw_2NNWwLct7onsRGbQNNYTzs"}

def sato_query(query):
    for i in range(2):
        payload = requests.get(api_endpoints[i] + query, headers=headers, verify=True)
        if payload.status_code == 200:
            return payload
    return None

def type_to_entrypoint(entity_type):
    return {
        "Device": "devices",
        "Building": "buildings",
        "Zone": "zones",
        "Space": "spaces",
        "Site": "sites",
        "External": "external",
        "Task": "tasks"
    }[entity_type]

def list_entities(entity_type, idPattern=None, q=None, options=None):
    endpoint = type_to_entrypoint(entity_type=entity_type)
    query = f"/{endpoint}?"

    if idPattern is not None:
        query += f"idPattern=.*{idPattern}.*"
    if q is not None:
        query += f"&q={quote_plus(q)}"   # URL-encode the q expression
    if options is not None:
        query += f"&options={options}"

    offset = 0
    all_entities = []
    while True:
        entity = sato_query(query + f"&offset={offset}")
        if entity and entity.status_code == 200:
            batch = entity.json()
            if not batch:
                return all_entities
            all_entities += batch
            offset += 100
        else:
            raise Exception(f"FAILED to get entity info: {query}")

# ---------- NEW utility: fetch tasks for a list of q-expressions ----------
def fetch_tasks_by_categories(q_exprs):
    tasks = []
    for q in q_exprs:
        tasks.extend(list_entities("Task", q=q, options="full"))
    return tasks

# ---------- NEW: save generic categories (Assessment, KPI) ---------------
def fetch_and_save_generic_tasks(filename="sato_generic_tasks.xlsx"):
    q_exprs_generic = [
        'category=="Assessment"',
        'category=="KPI"'
    ]
    tasks = fetch_tasks_by_categories(q_exprs_generic)

    # Flatten & save
    rows = []
    for t in tasks:
        flat = {"type": "Task"}
        flat.update(t)
        rows.append(flat)

    df = pd.DataFrame(rows)
    df.to_excel(filename, index=False, engine="openpyxl")
    print(f"Saved {len(df)} task rows to {filename}")

# ---------- NEW: save instance categories (Assessment/KPI/Service -Instance) ----------
def fetch_and_save_instance_tasks(filename="Task-Instance-Retrieval.xlsx"):
    q_exprs_instances = [
        'category=="Assessment-Instance"',
        'category=="KPI-Instance"',
        'category=="Service-Instance"'
    ]
    tasks = fetch_tasks_by_categories(q_exprs_instances)

    # Flatten & save
    rows = []
    for t in tasks:
        flat = {"type": "Task"}
        flat.update(t)
        rows.append(flat)

    df = pd.DataFrame(rows)
    df.to_excel(filename, index=False, engine="openpyxl")
    print(f"Saved {len(df)} task rows to {filename}")

fetch_and_save_generic_tasks()                # -> sato_generic_tasks.xlsx
fetch_and_save_instance_tasks()   
