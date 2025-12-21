# filepath: general_task_matcher.py
import json
import re
import os
import copy
import pandas as pd
from typing import List, Dict, Any, Iterable

# ============================================================
# =============== PREFIX TREE IMPLEMENTATION =================
# ============================================================

class Node:
    def __init__(self, value):
        self.value = value
        self.children = {}
        self.is_end = False


class PrefixTree:
    def __init__(self):
        self.root = Node(None)
    
    def insert(self, word):
        current_node = self.root
        for char in word:
            if char not in current_node.children:
                current_node.children[char] = Node(char)
            current_node = current_node.children[char]
        current_node.is_end = True
    
    def collect_all_ids(self, node=None) -> list:
        """Traverse the tree and collect all complete words (IDs)."""
        node = node or self.root
        all_ids = []

        def dfs(n, path):
            new_path = path + (n.value or "")
            if n.is_end:
                all_ids.append(new_path)
            for child in n.children.values():
                dfs(child, new_path)

        dfs(node, "")
        return all_ids


def load_ids_and_insert_into_tree(filename, prefix_tree, sheet_name=0, id_column="id", strip=True, case=None):
    """Loads entity IDs from Excel and inserts them into the prefix tree."""
    df = pd.read_excel(filename, sheet_name=sheet_name, usecols=[id_column])
    s = df[id_column].dropna().astype(str)
    if strip:
        s = s.str.strip()
    if case == "upper":
        s = s.str.upper()
    elif case == "lower":
        s = s.str.lower()
    for id_str in s[s.ne("")].drop_duplicates():
        prefix_tree.insert(id_str)

def print_prefix_tree(node, prefix):
    if node.is_end:
        print(prefix)
    for char, child_node in node.children.items():
        print_prefix_tree(child_node, prefix + char)

# ============================================================
# =============== CONFIGURATION & HELPERS ====================
# ============================================================

TOKEN = r"(?:^|[^A-Za-z]){tok}(?:[^A-Za-z]|$)"
REQ_BLOCK_MODE = "all"     #  FIXED: Require all blocks (SpacialContext + TotalLoad)
VERBOSE = True


def collect_all_ids(node):
    """Recursively collect all entity IDs from a prefix tree."""
    out = []
    def dfs(n, path):
        full_path = path + (getattr(n, "value", "") or "")
        if getattr(n, "is_end", False):
            out.append(full_path)
        for _, child in n.children.items():
            dfs(child, full_path)
    dfs(node, "")
    return out





# ============================================================
# =============== AUTO-DISCOVERY (GENERIC) ===================
# ============================================================

def extract_tokens_from_tasks(tasks: List[Dict[str, Any]]) -> List[str]:
    """Dynamically extract all unique string tokens from the 'requires' sections."""
    tokens = set()
    for task in tasks:
        requires = task.get("requires", {})
        for block in iter_requirement_blocks(requires):
            for key, val in block.items():
                if isinstance(val, list):
                    tokens.update(map(str, val))
                elif isinstance(val, str):
                    tokens.add(val)
                elif isinstance(val, dict):
                    tokens.update(map(str, val.values()))
    return list(tokens)


def infer_requirement_keys(tasks: List[Dict[str, Any]]) -> set:
    """Infer what keys appear under 'requires' across all tasks."""
    keys = set()
    for t in tasks:
        req = t.get("requires")
        for block in iter_requirement_blocks(req):
            keys.update(block.keys())
    return keys


# ============================================================
# =============== REQUIREMENT MATCHING (FIXED) ===============
# ============================================================

def build_requirement_groups(req_details: Dict[str, Any], recognized_keys: set) -> List[List[str]]:
    """Converts a requirement block into groups of tokens."""
    groups = []
    for key, vals in req_details.items():
        if key not in recognized_keys:
            continue
        if not isinstance(vals, list):
            vals = [vals]
        groups.append([str(v) for v in vals])
    return groups


def requirement_value_matches(id_str: str, value: str) -> bool:
    """Normalize separators and check if value appears in the ID."""
    id_norm = re.sub(r"[^a-z0-9]+", "", id_str.lower())
    val_norm = re.sub(r"[^a-z0-9]+", "", str(value).lower())
    return val_norm in id_norm


def id_satisfies_requirement_block(id_str: str, req_block: Dict[str, Any], recognized_keys: set) -> bool:
    """Check if entity ID satisfies all tokens within one requirement block."""
    groups = build_requirement_groups(req_block, recognized_keys)
    if not groups:
        return True
    for group in groups:
        if not any(requirement_value_matches(id_str, val) for val in group):
            return False
    return True


def id_satisfies_requirements(id_str: str, requirements: Any, recognized_keys: set) -> bool:
    """For each requirement block, check if the entity satisfies its tokens."""
    blocks = list(iter_requirement_blocks(requirements))
    if not blocks:
        return True
    if REQ_BLOCK_MODE == "all":
        return all(id_satisfies_requirement_block(id_str, b, recognized_keys) for b in blocks)
    else:
        return any(id_satisfies_requirement_block(id_str, b, recognized_keys) for b in blocks)


def id_mentions_any_token(id_str: str, tokens: List[str]) -> bool:
    """Quick prefilter: return True if entity ID contains at least one token."""
    id_norm = re.sub(r"[^a-z0-9]+", "", id_str.lower())
    for tok in tokens:
        tok_norm = re.sub(r"[^a-z0-9]+", "", str(tok).lower())
        if tok_norm and tok_norm in id_norm:
            return True
    return False


# ============================================================
# =============== TASK INSTANCE CREATION =====================
# ============================================================

def _safe_id_fragment(s: str, max_len: int = 120) -> str:
    frag = re.sub(r"[^A-Za-z0-9_.:/\\-]+", "-", s)
    frag = re.sub(r"-{2,}", "-", frag).strip("-")
    if len(frag) > max_len:
        frag = frag[:max_len].rstrip("-")
    return frag or "entity"


def add_entity_to_requires(requires: Any, entity_id: str) -> Any:
    """Insert the matched entity id under 'requires'."""
    if isinstance(requires, dict):
        new_req = copy.deepcopy(requires)
        key = "entityId" if "entityId" not in new_req else "_entityId"
        new_req[key] = entity_id
        return new_req
    if isinstance(requires, list):
        new_req = copy.deepcopy(requires)
        new_req.append({"entityId": entity_id})
        return new_req
    return {"entityId": entity_id}


def make_task_instance(base_task: Dict[str, Any], entity_id: str) -> Dict[str, Any]:
    """Clone the base task and inject the entity_id."""
    t = copy.deepcopy(base_task)
    base_name = t.get("name") or "Task"
    base_id = t.get("taskId") or (t.get("id") or "T")
    frag = _safe_id_fragment(entity_id)
    t["name"] = f"{base_name}  {entity_id}"
    t["taskId"] = f"{base_id}::{frag}"
    t["requires"] = add_entity_to_requires(t.get("requires"), entity_id)
    return t


def _unique_path(base_dir: str, filename_no_ext: str) -> str:
    """Ensure we don't overwrite existing files."""
    path = os.path.join(base_dir, f"{filename_no_ext}.json")
    if not os.path.exists(path):
        return path
    i = 2
    while True:
        candidate = os.path.join(base_dir, f"{filename_no_ext}_{i}.json")
        if not os.path.exists(candidate):
            return candidate
        i += 1


def save_task_instance(instance: Dict[str, Any], out_dir: str) -> str:
    """Save a single task instance to its own JSON file."""
    os.makedirs(out_dir, exist_ok=True)
    base = instance.get("taskId") or instance.get("name") or "task"
    fname = _safe_id_fragment(base)
    out_path = _unique_path(out_dir, fname)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(instance, f, indent=2, ensure_ascii=False)
    return out_path


# ============================================================
# =============== MAIN GENERIC DRIVER (FIXED) ================
# ============================================================

def match_task_requirements(root_node, task_json_path, out_dir="expanded_tasks_per_entity"):
    """
    Generic + Fixed task matcher:
    - Reads any task JSON (with arbitrary 'requires' structure)
    - Learns requirement keys and tokens automatically
    - Matches entities per-requirement (normalized & filtered)
    - Saves one file per matched task-entity instance
    """
    all_ids = collect_all_ids(root_node)

    with open(task_json_path, "r", encoding="utf-8") as f:
        tasks = json.load(f)

    #  FIX 1: Filter only meaningful semantic keys
    recognized_keys = {
        k for k in infer_requirement_keys(tasks)
        if k.lower() in {
            "type",
            "category",
            "class",
            "measure",
            "measuresproperty",
            "measuredproperty",
            "controlsproperty"
        }
    }

    # Extract all task tokens for quick pre-filtering
    extracted_tokens = extract_tokens_from_tasks(tasks)

    if VERBOSE:
        print(f"\nDiscovered requirement keys: {recognized_keys}")
        print(f"Extracted tokens: {extracted_tokens}\n")

    total_saved = 0

    for task in tasks:
        task_name = task.get("name")
        task_id = task.get("taskId") or task.get("id")
        requirements = task.get("requires")

        if VERBOSE:
            print(f"\n---\nProcessing Task: {task_name}\nTask ID: {task_id}")

        matched_ids = []
        for id_str in all_ids:
            # Optional quick pre-filter
            if not id_mentions_any_token(id_str, extracted_tokens):
                continue
            #  Strong semantic matching
            if id_satisfies_requirements(id_str, requirements, recognized_keys):
                matched_ids.append(id_str)

        if not matched_ids:
            print("No matches found.")
            continue

        print(f"Matched {len(matched_ids)} entities:")
        for id_str in matched_ids:
            instance = make_task_instance(task, id_str)
            out_path = save_task_instance(instance, out_dir)
            total_saved += 1
            if VERBOSE:
                print(f"  → saved: {out_path}")

    print(f"\n Saved {total_saved} task instances to: {out_dir}")
