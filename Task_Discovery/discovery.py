import neo4j
from neo4j import GraphDatabase, basic_auth
from sato_function import sato_query, list_entities, type_to_entrypoint, create_node

# Replace these with your actual credentials
NEO4J_URI = "neo4j://172.19.0.2:7687"
USERNAME = 'neo4j'
PASSWORD = 'password' 

# Create a Neo4j driver instance
driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(USERNAME, PASSWORD))
# SATO 
api_endpoints = ["https://project-sato1.lasige.di.fc.ul.pt/api", "https://project-sato2.lasige.di.fc.ul.pt/api"]
headers = {"x-api-key": "sato_1oyQx-XWRQ1tq7VjLxIw_2NNWwLct7onsRGbQNNYTzs"}

#Create all entities
entity_types = ["Building", "Space", "Zone", "Device", "External", "Task"]

with driver.session() as session:
    for entity_type in entity_types:
        print("Fetching all entities of type:", entity_type)
        entities = list_entities(entity_type=entity_type, options="full")

        print("Populating graph...")
        for entity in entities:
            
            # Init with ID and Type as they are special properties
            entity_id = entity["id"]
            if("Berkeley" in entity_id): continue
            
            entity_type = entity["type"]
            params = {"id": entity_id, "type": entity_type}

            # Build the query depending on existing data
            query = [f"CREATE (n: {entity_type}", "{id: $id, type: $type"]

            for prop, value in entity.items():
                # Skip ID and Type since we already have them above
                if(prop in ["id", "type"]): continue

                # If the prop is a list, then its a relationship and we can skip it
                if (type(value) == list):
                    continue
                if (type(value) != dict):
                    raise Exception("Yo wtf? There is prop that is not a dict besides ID and Type?")

                # Now that we are sure its a dict, lets make sure it is not a relationship
                if (value["type"] == "Relationship" or prop == "requires"):
                    continue

                # Finally Add the property
                query += [f",{prop}: ${prop}"]
                params[prop] = str(value["value"])

            # Close the query
            query += ["})"]
            

            final_query = "".join(query)
            session.execute_write(create_node, final_query, params)


# Link entities using Relationships

with driver.session() as session:
    for entity_type in entity_types:
        print("Fetching all entities of type:", entity_type)
        entities = list_entities(entity_type=entity_type, options="full")

        print("Populating graph...")
        for entity in entities:

            # Init with ID and Type as they are special properties
            entity_id = entity["id"]
            if ("Berkeley" in entity_id):
                continue

            entity_type = entity["type"]

            for prop, value in entity.items():
                # Skip ID and Type
                if (prop in ["id", "type"]):
                    continue

                # If the prop is a list, then its a relationship
                if (type(value) == list):
                    for target_entity in value:
                        target_id = target_entity["object"]
                        target_type = target_id.split(":")[2]
                        
                        print(entity_id, "--", prop, "-->", target_id)

                        query = f"MATCH (src: {entity_type} " + "{id: $src_id}),"
                        query += f"(tgt: {target_type}" + "{id: $tgt_id}) "
                        query += f"CREATE (src)-[:{prop}]->(tgt)"

                        session.execute_write(create_node, query, {"src_id": entity_id, "tgt_id": target_id})
                        
                elif(value["type"] == "Relationship"):
                    target_id = value["object"]
                    target_type = target_id.split(":")[2]

                    print(entity_id, "--", prop, "-->", target_id)
                    
                    query = f"MATCH (src: {entity_type} " + "{id: $src_id}),"
                    query += f"(tgt: {target_type}" + "{id: $tgt_id}) "
                    query += f"CREATE (src)-[:{prop}]->(tgt)"

                    session.execute_write(create_node, query, {"src_id": entity_id, "tgt_id": target_id})

# TODO: Add a new elif for when prop == "requires"
                elif prop == "requires":
                    for req_k, req_v in value["value"].items():
                        # Ensure req_v is always a list (even if it's a single dict)
                        req_list = req_v if isinstance(req_v, list) else [req_v]

                        # Now iterate over the list uniformly
                        for v in req_list:
                            # Skip if there is no id in this element
                            if not isinstance(v, dict) or "id" not in v:
                                continue

                            full_id = "urn:ngsi-ld:" + v["type"] + ":" + v["id"]
                            rel = ("reqAs_" + req_k).replace("-", "_")
                            print(full_id, rel)


                            query = f"MATCH (src: {entity_type} " + "{id: $src_id}),"
                            query += f"(tgt: {v['type']}" + "{id: $tgt_id}) "
                            query += f"CREATE (src)-[:{rel}]->(tgt)"

                            session.execute_write(create_node, query, {
                                                "src_id": entity_id, "tgt_id": full_id})


# # Example function to test the connection and run a simple query
# def test_connection():
#     with driver.session() as session:
#         result = session.run("RETURN 'Hello from Neo4j!' AS message")
#         for record in result:
#             print(record["message"])

# if __name__ == "__main__":
#     try:
#         test_connection()
#     finally:
#         driver.close()