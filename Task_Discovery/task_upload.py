import json
import hashlib
import os
from neo4j import GraphDatabase

# -----------------------------
# Neo4j config
# -----------------------------
NEO4J_URI = "neo4j://172.19.0.2:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

PRIMITIVES = (str, int, float, bool)

# -----------------------------
# TaskIds that MUST be overridden (generic tasks only)
# -----------------------------
OVERRIDE_TASK_IDS = {
    "A28-S2-G-F-V05",
    "A53",
    "A01",
    "A28-K:SC000-V01",
    "A28-K:SC000-V03",
    "A28-K:BP100-V03",
    "A28-K:BP100-V01",
    "A28-K:EP100-V01",
    "A28-K:EP200-V01",
    "A01-K:00001-V01",
    "A28-K:EP100-V02",
    "A28-K:SC130-V01",
    "A28-K:BP100-V02",
    "A28-K:SC000-V02",
    "A28-K:SC000-V04",
    "A28-K:SC500-V01",
    "A28-K:SC500-V04",
    "A28-K:SC500-V02",
    "A28-K:SC500-V05",
    "VS01-V01",
}

GENERIC_OVERRIDE_CATEGORIES = {"KPI", "Assessment", "Service", "VirtualSensor"}


# -----------------------------
# Helpers: normalization
# -----------------------------
def normalize_value(v):
    """
    Convert Python values into Neo4j-safe property values.
    Allowed:
      - str, int, float, bool, None
      - list of those
    Everything else → JSON string.
    """
    if v is None:
        return None
    if isinstance(v, PRIMITIVES):
        return v
    if isinstance(v, list):
        # list of primitives / None is OK
        if all(isinstance(x, PRIMITIVES) or x is None for x in v):
            return v
        return json.dumps(v, sort_keys=True)
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True)
    return str(v)


def normalize_map(d: dict) -> dict:
    return {k: normalize_value(v) for k, v in d.items()}


# -----------------------------
# Helpers: task id + signature
# -----------------------------
def task_key_from_task(task: dict):
    """
    Logical identifier for a Task in JSON:
      Prefer taskId if present, otherwise fall back to id.
    """
    return task.get("taskId") or task.get("id")


def task_keys_from_req(req_value: dict):
    """
    Return a list of taskIds referenced by this 'requires' entry.

    Supports:
      - taskId or id as a single value
      - taskId or id as a list of values
      - taskId or id as a stringified JSON list
      - list elements that are dicts with their own taskId/id
    """
    raw = req_value.get("taskId")
    if raw is None:
        raw = req_value.get("id")

    if raw is None:
        return []

    if isinstance(raw, list):
        candidates = raw
    else:
        if isinstance(raw, str):
            s = raw.strip()
            if s.startswith("[") and s.endswith("]"):
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, list):
                        candidates = parsed
                    else:
                        candidates = [parsed]
                except Exception:
                    candidates = [raw]
            else:
                candidates = [raw]
        else:
            candidates = [raw]

    task_ids = []
    for item in candidates:
        if isinstance(item, dict):
            key = item.get("taskId") or item.get("id")
            if key is not None:
                task_ids.append(str(key))
        else:
            if item is not None:
                task_ids.append(str(item))

    return [k for k in task_ids if k]


def requirement_signature(key, req_value):
    canonical = json.dumps(req_value, sort_keys=True)
    return hashlib.sha256(f"{key}|{canonical}".encode("utf-8")).hexdigest()


def is_task_reference(req_value: dict) -> bool:
    if not isinstance(req_value, dict):
        return False
    if req_value.get("type") != "Task":
        return False
    return ("taskId" in req_value) or ("id" in req_value)


def iter_task_reference_dicts(obj):
    if isinstance(obj, dict):
        if is_task_reference(obj):
            yield obj
        for v in obj.values():
            yield from iter_task_reference_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_task_reference_dicts(item)


# ============================================================
# PASS 1 — create all Task nodes (no relationships)
# ============================================================
def _create_task_node_only(tx, task: dict):
    """
    Create/merge Task nodes.

    Rules:

      - INSTANCE tasks (category ends with '-Instance'):
          -> use 'id' as the unique key (MERGE on id),
             ON CREATE SET properties,
             ON MATCH do nothing (leave existing instance as-is).
             (We never touch existing instances.)

      - NON-instance tasks:
          -> never use 'id' (to avoid colliding with instance ids).
          -> MERGE by (taskId, category) so we never match instances.

          - If taskId in OVERRIDE_TASK_IDS OR category in GENERIC_OVERRIDE_CATEGORIES:
                full overwrite (SET t = $props).
            Else:
                additive update (SET t += $props).
    """
    # Base properties without 'requires'
    base_props = {k: v for k, v in task.items() if k != "requires"}

    # Logical ID
    base_id = task_key_from_task(task)
    if not base_id:
        raise ValueError(f"Task has neither 'taskId' nor 'id': {task}")

    base_id_str = str(base_id)
    category = task.get("category")

    is_instance_category = isinstance(category, str) and category.endswith("-Instance")

    # -------- 1) INSTANCE TASKS --------
    if is_instance_category:
        # For instances, 'id' is the unique key under your :Task(id) constraint
        instance_id = task.get("id") or base_id_str
        instance_id_str = str(instance_id)

        # Ensure 'id' is in props; taskId can also be present if JSON has it
        base_props["id"] = instance_id_str
        if "taskId" not in base_props and task.get("taskId"):
            base_props["taskId"] = str(task["taskId"])

        props = normalize_map(base_props)

        tx.run(
            """
            MERGE (t:Task {id: $id})
            ON CREATE SET t = $props
            """,
            id=instance_id_str,
            props=props,
        )
        return

    # -------- 2) NON-INSTANCE TASKS (GENERIC + OTHERS) --------
    # Never set 'id' on generic/non-instance tasks
    base_props.pop("id", None)

    # Ensure taskId/category present
    if "taskId" not in base_props:
        base_props["taskId"] = base_id_str
    if category is not None:
        base_props["category"] = category

    props = normalize_map(base_props)

    override_by_category = category in GENERIC_OVERRIDE_CATEGORIES
    override_by_id = base_id_str in OVERRIDE_TASK_IDS

    # We always MERGE generics by (taskId, category) when category is known,
    # so we never collide with instances (which have category '...-Instance').
    if override_by_category or override_by_id:
        # FULL overwrite
        if category is not None:
            tx.run(
                """
                MERGE (t:Task {taskId: $taskId, category: $category})
                SET t = $props
                """,
                taskId=base_id_str,
                category=category,
                props=props,
            )
        else:
            tx.run(
                """
                MERGE (t:Task {taskId: $taskId})
                SET t = $props
                """,
                taskId=base_id_str,
                props=props,
            )
    else:
        # Additive update
        if category is not None:
            tx.run(
                """
                MERGE (t:Task {taskId: $taskId, category: $category})
                SET t += $props
                """,
                taskId=base_id_str,
                category=category,
                props=props,
            )
        else:
            tx.run(
                """
                MERGE (t:Task {taskId: $taskId})
                SET t += $props
                """,
                taskId=base_id_str,
                props=props,
            )


# ============================================================
# PASS 2 — create Requirement nodes and requires relationships
# ============================================================
def _create_requirements_and_links(tx, task: dict):

    requires = task.get("requires", {})
    if not isinstance(requires, dict):
        return

    category = task.get("category")
    is_instance_category = isinstance(category, str) and category.endswith("-Instance")

    # Do NOT touch task instances in this phase:
    if is_instance_category:
        return

    # Use logical "taskId" (or id) as identifier for SOURCE,
    # but in MATCH we will constrain by category as well to avoid hitting instances.
    this_task_id = task_key_from_task(task)
    if not this_task_id:
        return

    this_task_id = str(this_task_id)
    src_category = category  # may be None for some generics

    for req_key, req_value in requires.items():

        # Normalise non-dict/list values
        if not isinstance(req_value, (dict, list)):
            req_value = {"value": req_value}

        # -------- CASE A: handle ALL nested Task references (recursive) --------
        for task_ref in iter_task_reference_dicts(req_value):
            raw_target_ids = task_keys_from_req(task_ref)
            if not raw_target_ids:
                continue

            # Source is ALWAYS the generic task of this JSON (not an instance),
            # so we match by (taskId, category) when category is known.
            # IMPORTANT: target must be a GENERIC task, not an instance:
            #   - tgt.category IS NULL OR NOT tgt.category ENDS WITH '-Instance'
            if src_category is not None:
                tx.run(
                    """
                    MATCH (src:Task {taskId: $src_id, category: $src_category})
                    UNWIND $tgt_ids AS tid
                    MATCH (tgt:Task {taskId: tid})
                    WHERE tgt.category IS NULL
                       OR NOT tgt.category ENDS WITH '-Instance'
                    MERGE (src)-[:requires]->(tgt)
                    """,
                    src_id=this_task_id,
                    src_category=src_category,
                    tgt_ids=raw_target_ids,
                )
            else:
                tx.run(
                    """
                    MATCH (src:Task {taskId: $src_id})
                    UNWIND $tgt_ids AS tid
                    MATCH (tgt:Task {taskId: tid})
                    WHERE tgt.category IS NULL
                       OR NOT tgt.category ENDS WITH '-Instance'
                    MERGE (src)-[:requires]->(tgt)
                    """,
                    src_id=this_task_id,
                    tgt_ids=raw_target_ids,
                )

        # If the TOP-LEVEL value itself is a pure Task reference,
        # we do NOT create a Requirement node for it.
        if isinstance(req_value, dict) and is_task_reference(req_value):
            continue

        # -------- CASE B: normal Requirement node (non-Task) --------
        key_str = str(req_key)

        # Build base properties, but treat list-valued "type" separately
        raw_req_props = {"key": key_str}
        if isinstance(req_value, dict):
            raw_req_props.update(req_value)
        else:
            raw_req_props["value"] = req_value

        # Optional: keep a signature for debugging/uniqueness checks
        raw_req_props["signature"] = requirement_signature(req_key, req_value)

        # Extract and normalise "type" explicitly so we can force list behaviour
        type_list = None
        if isinstance(req_value, dict) and "type" in req_value:
            tval = req_value["type"]
            if isinstance(tval, list):
                type_list = [str(x) for x in tval]
            else:
                # scalar → wrap in list
                type_list = [str(tval)]
            # remove from generic props; we will SET/MERGE it explicitly
            raw_req_props.pop("type", None)

        req_props = normalize_map(raw_req_props)

        # 1) Merge/Create Requirement by key with all other props
        tx.run(
            """
            MERGE (r:Requirement {key: $key})
            SET r += $props
            """,
            key=key_str,
            props=req_props,
        )

        # 2) Explicitly set/merge the list-valued "type" property if present
        if type_list is not None:
            tx.run(
                """
                MATCH (r:Requirement {key: $key})
                WITH r, $type_list AS newTypes
                // Old values (or only newTypes if r.type is null)
                WITH r,
                     CASE
                       WHEN r.type IS NULL THEN newTypes
                       ELSE r.type + newTypes
                     END AS allTypes
                // Deduplicate while preserving order
                WITH r,
                     reduce(acc = [], x IN allTypes |
                       CASE WHEN x IN acc THEN acc ELSE acc + x END
                     ) AS merged
                SET r.type = merged
                """,
                key=key_str,
                type_list=type_list,
            )

        # 3) Link the generic Task -> Requirement
        if src_category is not None:
            tx.run(
                """
                MATCH (t:Task {taskId: $taskId, category: $category}),
                      (r:Requirement {key: $key})
                MERGE (t)-[:requires]->(r)
                """,
                taskId=this_task_id,
                category=src_category,
                key=key_str,
            )
        else:
            tx.run(
                """
                MATCH (t:Task {taskId: $taskId}),
                      (r:Requirement {key: $key})
                MERGE (t)-[:requires]->(r)
                """,
                taskId=this_task_id,
                key=key_str,
            )


# -----------------------------
# JSON loading helpers
# -----------------------------
def load_tasks_from_file(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]


def collect_all_tasks_from_folder(folder: str):
    all_tasks = []
    for fname in os.listdir(folder):
        if not fname.lower().endswith(".json"):
            continue
        full_path = os.path.join(folder, fname)
        if not os.path.isfile(full_path):
            continue
        all_tasks.extend(load_tasks_from_file(full_path))
    return all_tasks


# -----------------------------
# Main bulk loader
# -----------------------------
def load_tasks_from_folder(folder: str):
    all_tasks = collect_all_tasks_from_folder(folder)

    with driver.session() as session:
        # PASS 1: create/update Task nodes (respecting instance rules)
        for t in all_tasks:
            session.execute_write(_create_task_node_only, t)

    with driver.session() as session:
        # PASS 2: create Requirement nodes + requires relationships
        for t in all_tasks:
            session.execute_write(_create_requirements_and_links, t)


# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    TASKS_FOLDER = "all_task"  # adjust as needed
    load_tasks_from_folder(TASKS_FOLDER)
    driver.close()
    print("Finished loading tasks and requirements into Neo4j.")
