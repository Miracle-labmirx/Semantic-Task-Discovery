from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4
import os
from .neo4j_gateway import Neo4jGateway
from .schema import GraphSchema


@dataclass
class RequirementBinding:
    requirement_eid: str
    requirement_id: Optional[str]
    entity_id: str


@dataclass
class DependencyBinding:
    requirement_id: Optional[str]
    dependency_task_id: str
    dependency_instance_id: str


@dataclass
class DiscoveryDecision:
    building_id: str
    context_id: str
    context_level: str
    task_id: str
    instantiated: bool
    reason: str
    created_instance_id: Optional[str] = None
    output_file: Optional[str] = None
    requirement_bindings: List[Dict[str, Any]] = field(default_factory=list)
    dependency_bindings: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class TopologyEntity:
    entity_id: str
    labels: List[str]
    properties: Dict[str, Any]


@dataclass
class RequirementTargetMatch:
    target_node_id: Optional[str]
    target_task_id: Optional[str]
    target_labels: List[str]
    target_properties: Dict[str, Any]
    required_properties: Dict[str, Any]
    satisfied: bool
    matched_entity_ids: List[str] = field(default_factory=list)


@dataclass
class RequirementResolution:
    requirement_eid: str
    requirement_id: Optional[str]
    satisfied: bool
    matched_entity_ids: List[str] = field(default_factory=list)
    satisfied_dependency_task_ids: List[str] = field(default_factory=list)
    target_matches: List[RequirementTargetMatch] = field(default_factory=list)
    reason: Optional[str] = None


@dataclass
class DirectDependencyResolution:
    dependency_task_id: str
    satisfied: bool
    reason: Optional[str] = None


@dataclass
class TaskResolution:
    task_id: str
    task_name: Optional[str]
    satisfied: bool
    matched_entity_ids: List[str] = field(default_factory=list)
    requirements: List[RequirementResolution] = field(default_factory=list)
    direct_dependencies: List[DirectDependencyResolution] = field(default_factory=list)
    dependency_chain: List["TaskResolution"] = field(default_factory=list)
    dependency_cycles: List[List[str]] = field(default_factory=list)
    reason: Optional[str] = None


class SemanticTaskDiscoverer:
    """Stage-2 semantic discovery using Neo4j graph traversal and Cypher queries."""

    def __init__(self, gateway: Neo4jGateway, schema: GraphSchema, output_root: str = "generated_task_instances") -> None:
        self.gateway = gateway
        self.schema = schema
        self.output_root = Path(output_root)
        self.output_root.mkdir(parents=True, exist_ok=True)

    def discover_for_building(self, building_id: str) -> List[DiscoveryDecision]:
        contexts = self.gateway.get_building_contexts(building_id)
        generic_tasks = self.gateway.get_generic_tasks()
        decisions: List[DiscoveryDecision] = []

        # Cache to avoid repeated recursive evaluations.
        resolution_cache: Dict[Tuple[str, str], Optional[Tuple[List[RequirementBinding], List[DependencyBinding]]]] = {}

        for ctx in contexts:
            context_id = ctx["context_id"]
            context_level = ctx["context_level"]
            for task in generic_tasks:
                task_id = task["task_id"]
                if self.gateway.context_has_instance(context_id=context_id, task_id=task_id):
                    decisions.append(
                        DiscoveryDecision(
                            building_id=building_id,
                            context_id=context_id,
                            context_level=context_level,
                            task_id=task_id,
                            instantiated=True,
                            reason="already_instantiated",
                        )
                    )
                    continue

                resolved = self._resolve_task_requirements(
                    context_id=context_id,
                    task_id=task_id,
                    visited=set(),
                    cache=resolution_cache,
                )
                if not resolved:
                    decisions.append(
                        DiscoveryDecision(
                            building_id=building_id,
                            context_id=context_id,
                            context_level=context_level,
                            task_id=task_id,
                            instantiated=False,
                            reason="requirements_not_satisfied",
                        )
                    )
                    continue

                requirement_bindings, dependency_bindings = resolved
                instance_id = str(uuid4())
                instance_label = self._instance_label_for_task(task)
                payload = self._build_instance_properties(
                    task=task,
                    building_id=building_id,
                    context_id=context_id,
                    context_level=context_level,
                    instance_id=instance_id,
                    requirement_bindings=requirement_bindings,
                    dependency_bindings=dependency_bindings,
                )
                self.gateway.create_task_instance(
                    generic_task_id=task_id,
                    instance_id=instance_id,
                    context_id=context_id,
                    instance_label=instance_label,
                    properties=payload,
                    requirement_bindings=[
                        {
                            "requirement_eid": b.requirement_eid,
                            "requirement_id": b.requirement_id,
                            "entity_id": b.entity_id,
                        }
                        for b in requirement_bindings
                    ],
                    dependency_bindings=[
                        {
                            "requirement_id": b.requirement_id,
                            "dependency_task_id": b.dependency_task_id,
                            "dependency_instance_id": b.dependency_instance_id,
                        }
                        for b in dependency_bindings
                    ],
                )
                output_file = self._write_instance_file(
                    building_id=building_id,
                    context_level=context_level,
                    context_id=context_id,
                    task_id=task_id,
                    payload=payload,
                )
                decisions.append(
                    DiscoveryDecision(
                        building_id=building_id,
                        context_id=context_id,
                        context_level=context_level,
                        task_id=task_id,
                        instantiated=True,
                        reason="instantiated_by_semantic_discovery",
                        created_instance_id=instance_id,
                        output_file=str(output_file),
                        requirement_bindings=[
                            {
                                "requirement_id": b.requirement_id,
                                "requirement_eid": b.requirement_eid,
                                "entity_id": b.entity_id,
                            }
                            for b in requirement_bindings
                        ],
                        dependency_bindings=[
                            {
                                "requirement_id": b.requirement_id,
                                "dependency_task_id": b.dependency_task_id,
                                "dependency_instance_id": b.dependency_instance_id,
                            }
                            for b in dependency_bindings
                        ],
                    )
                )

        return decisions

    def _resolve_task_requirements(
        self,
        context_id: str,
        task_id: str,
        visited: Set[Tuple[str, str]],
        cache: Dict[Tuple[str, str], Optional[Tuple[List[RequirementBinding], List[DependencyBinding]]]],
    ) -> Optional[Tuple[List[RequirementBinding], List[DependencyBinding]]]:
        cache_key = (context_id, task_id)
        if cache_key in cache:
            return cache[cache_key]
        if cache_key in visited:
            # Circular dependency in task requirements.
            return None

        visited.add(cache_key)
        requirements = self.gateway.get_task_requirements(task_id)
        requirement_bindings: List[RequirementBinding] = []
        dependency_bindings: List[DependencyBinding] = []

        for req in requirements:
            req_eid = req.get("requirement_eid")
            req_id = req.get("requirement_id")
            req_props = req.get("requirement_properties") or {}
            targets = req.get("targets") or []
            if not targets:
                # A disconnected requirement cannot be evaluated safely.
                visited.remove(cache_key)
                cache[cache_key] = None
                return None

            satisfied = False
            for target in targets:
                labels = target.get("labels") or []
                target_task_id = target.get("task_id")

                # Requirement depends on another task.
                if self.schema.task_label in labels or target_task_id:
                    dep_task_id = target_task_id
                    if not dep_task_id:
                        continue

                    dep_instances = self.gateway.find_task_instances_for_context(context_id=context_id, task_id=dep_task_id)
                    if not dep_instances:
                        dep_resolved = self._resolve_task_requirements(
                            context_id=context_id,
                            task_id=dep_task_id,
                            visited=visited,
                            cache=cache,
                        )
                        if dep_resolved:
                            # Re-check after recursive resolution.
                            dep_instances = self.gateway.find_task_instances_for_context(
                                context_id=context_id,
                                task_id=dep_task_id,
                            )

                    if dep_instances:
                        dependency_bindings.append(
                            DependencyBinding(
                                requirement_id=req_id,
                                dependency_task_id=dep_task_id,
                                dependency_instance_id=dep_instances[0]["id"],
                            )
                        )
                        satisfied = True
                        break

                    continue

                # Requirement depends on physical/semantic entity.
                required_props = {}
                for key in ("entityType", "type", "name", "category", "measuresProperty", "controlsProperty", "unit"):
                    if req_props.get(key) is not None:
                        required_props[key] = req_props[key]
                candidates = self.gateway.find_entities_for_requirement(
                    context_id=context_id,
                    required_labels=labels,
                    required_props=required_props or None,
                )
                if candidates:
                    requirement_bindings.append(
                        RequirementBinding(
                            requirement_eid=req_eid,
                            requirement_id=req_id,
                            entity_id=candidates[0]["id"],
                        )
                    )
                    satisfied = True
                    break

            if not satisfied:
                visited.remove(cache_key)
                cache[cache_key] = None
                return None

        visited.remove(cache_key)
        result = (requirement_bindings, dependency_bindings)
        cache[cache_key] = result
        return result

    def _instance_label_for_task(self, task: Dict[str, Any]) -> str:
        labels = set(task.get("labels") or [])
        category = str((task.get("properties") or {}).get(self.schema.category_property, ""))

        if "VirtualSensor" in labels or "Virtual Sensor" in labels or category == "VirtualSensor":
            return "VirtualSensor-Instance"
        if "Assessment" in labels or category == "Assessment":
            return "Assessment-Instance"
        if "KPI" in labels or category == "KPI":
            return "KPI-Instance"
        return "Service-Instance"

    def _build_instance_properties(
        self,
        task: Dict[str, Any],
        building_id: str,
        context_id: str,
        context_level: str,
        instance_id: str,
        requirement_bindings: List[RequirementBinding],
        dependency_bindings: List[DependencyBinding],
    ) -> Dict[str, Any]:
        template_props = dict(task.get("properties") or {})
        template_props[self.schema.node_id_property] = instance_id
        template_props[self.schema.task_id_property] = task["task_id"]
        template_props[self.schema.category_property] = self._instance_label_for_task(task)
        template_props["isInstance"] = True
        template_props["instantiatedAt"] = datetime.now(tz=timezone.utc).isoformat()
        template_props["buildingId"] = building_id
        template_props["contextId"] = context_id
        template_props["contextLevel"] = context_level
        template_props["resolvedRequirements"] = [
            {
                "requirementId": b.requirement_id,
                "requirementElementId": b.requirement_eid,
                "entityId": b.entity_id,
            }
            for b in requirement_bindings
        ]
        template_props["resolvedTaskDependencies"] = [
            {
                "requirementId": b.requirement_id,
                "taskId": b.dependency_task_id,
                "instanceId": b.dependency_instance_id,
            }
            for b in dependency_bindings
        ]
        return template_props

    def _write_instance_file(
        self,
        building_id: str,
        context_level: str,
        context_id: str,
        task_id: str,
        payload: Dict[str, Any],
    ) -> Path:
        folder = self.output_root / building_id / context_level / context_id
        folder.mkdir(parents=True, exist_ok=True)
        file_path = folder / f"{task_id}__{payload.get(self.schema.node_id_property)}.json"
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        return file_path


class SemanticTaskResolver:
    """Read-only resolver for one task in one building."""

    _MATCHABLE_PROPERTY_KEYS = (
        "entityType",
        "type",
        "name",
        "category",
        "measuresProperty",
        "controlsProperty",
        "unit",
        "area",
    )

    def __init__(self, gateway: Neo4jGateway, schema: GraphSchema) -> None:
        self.gateway = gateway
        self.schema = schema

    def resolve_for_building_original(self, building_id: str, task_id: str) -> List[Dict[str, Any]]:
        chain_cache: Dict[str, Dict[str, Any]] = {}

        with ThreadPoolExecutor(max_workers=2) as executor:
            topology_future = executor.submit(self.gateway.get_building_topology, building_id)
            task_future = executor.submit(self._collect_task_chain, task_id, (), chain_cache)
            topology_rows = topology_future.result()
            root_chain = task_future.result()

        if not root_chain:
            return []

        topology_entities = [
            TopologyEntity(
                entity_id=row["id"],
                labels=list(row.get("labels") or []),
                properties=dict(row.get("properties") or {}),
            )
            for row in topology_rows
            if row.get("id")
        ]

        resolution_cache: Dict[str, TaskResolution] = {}
        root_resolution = self._evaluate_task(
            task_id=task_id,
            topology_entities=topology_entities,
            chain_cache=chain_cache,
            resolution_cache=resolution_cache,
            lineage=(),
        )

        return [
            self._build_task_payload(
                chain=chain_cache[task_id],
                resolution=root_resolution,
                chain_cache=chain_cache,
                resolution_cache=resolution_cache,
                lineage=(),
            )
        ]
    
    def resolve_for_building(self, building_id: str, task_id: str) -> List[Dict[str, Any]]:
        total_start = perf_counter()
        timings: Dict[str, Any] = {}
        chain_cache: Dict[str, Dict[str, Any]] = {}

        def timed_call(component_name: str, func, *func_args):
            start = perf_counter()
            value = func(*func_args)
            timings[component_name] = round(perf_counter() - start, 6)
            return value

        with ThreadPoolExecutor(max_workers=2) as executor:
            topology_future = executor.submit(
                timed_call,
                "building_context_extraction_seconds",
                self.gateway.get_building_topology,
                building_id,
            )
            task_future = executor.submit(
                timed_call,
                "task_requirement_construction_seconds",
                self._collect_task_chain,
                task_id,
                (),
                chain_cache,
            )
            topology_rows = topology_future.result()
            root_chain = task_future.result()

        if not root_chain:
            timings["total_execution_seconds"] = round(perf_counter() - total_start, 6)
            return {
                "task_id": task_id,
                "satisfied": False,
                "reason": "task_not_found",
                "execution_time_seconds": timings,
            }

        topology_entities_start = perf_counter()
        topology_entities = [
            TopologyEntity(
                entity_id=row["id"],
                labels=list(row.get("labels") or []),
                properties=dict(row.get("properties") or {}),
            )
            for row in topology_rows
            if row.get("id")
        ]
        timings["candidate_entity_pool_construction_seconds"] = round(perf_counter() - topology_entities_start, 6)

        # matched_entity_ids: List[str] = []

        # direct_dependencies: List[Dict[str, Any]] = root_chain.get("direct_dependencies", [])
        dependency_chain: Dict[str, List[str]] = root_chain.get("dependency_chain", {})
        print("the dependency chain is:", dependency_chain)
        # result = self.resolve_task_multiple_building(
        #     task_id=task_id,
        #     dependency_chain=dependency_chain,
        #     topology_entities=topology_entities,
        #     output_dir="resolution_results"
        # )
        result = self.resolve_task_multiple_building_revised(
            task_id=task_id,
            dependency_chain=dependency_chain,
            building_id=building_id,
            output_dir="resolution_results",
            timings=timings,
        )
        if isinstance(result, dict):
            result.setdefault("execution_time_seconds", timings)
            result["execution_time_seconds"]["total_execution_seconds"] = round(perf_counter() - total_start, 6)
        return result

    def resolve_task_multiple_building_revised(
        self,
        task_id: str,
        dependency_chain: Dict[str, List[str]],
        building_id: str,
        output_dir: str = "resolution_results",
        timings: Optional[Dict[str, Any]] = None,
    ):
        timings = timings if timings is not None else {}

        coverage_start = perf_counter()
        task_coverage= self.gateway.get_task_coverage_resolution_direct_deployment(building_id,task_id)[0]
        timings["task_coverage_resolution_seconds"] = round(perf_counter() - coverage_start, 6)

        not_launched_context_ids = task_coverage["not_launched_context_ids"]
        launched_context_ids = task_coverage["launched_context_ids"]
        all_results_not_launched = {}
        per_context_timings: Dict[str, Dict[str, float]] = {}
        device_retrieval_total = 0.0
        context_entity_pool_total = 0.0
        semantic_matching_total = 0.0

        for container_id in not_launched_context_ids:
            context_total_start = perf_counter()
            print(f"performing discovery for the space/zone/building: {container_id}")

            start = perf_counter()
            retrieved_devices = self.gateway.get_devices_in_container(container_id)
            device_retrieval_seconds = perf_counter() - start
            device_retrieval_total += device_retrieval_seconds

            start = perf_counter()
            topology_entities = [
            TopologyEntity(
                entity_id=row["id"],
                labels=list(row.get("labels") or []),
                properties=dict(row.get("properties") or {}),
            )
            for row in retrieved_devices
            if row.get("id")]
            entity_pool_seconds = perf_counter() - start
            context_entity_pool_total += entity_pool_seconds

            start = perf_counter()
            all_results_not_launched[container_id]=self.resolve_task_multiple_building(task_id,dependency_chain,topology_entities )
            semantic_matching_seconds = perf_counter() - start
            semantic_matching_total += semantic_matching_seconds

            per_context_timings[container_id] = {
                "device_retrieval_seconds": round(device_retrieval_seconds, 6),
                "candidate_entity_pool_construction_seconds": round(entity_pool_seconds, 6),
                "semantic_matching_validation_seconds": round(semantic_matching_seconds, 6),
                "total_context_seconds": round(perf_counter() - context_total_start, 6),
            }

        timings["device_retrieval_seconds"] = round(device_retrieval_total, 6)
        timings["context_entity_pool_construction_seconds"] = round(context_entity_pool_total, 6)
        timings["semantic_matching_validation_seconds"] = round(semantic_matching_total, 6)
        timings["per_context_seconds"] = per_context_timings

        evidence_start = perf_counter()
        instantiable_context_ids = []
        matched_devices = []
        zones = []
        spaces = []
        depends_on = dependency_chain.get(task_id) or None

        for container_id, result in all_results_not_launched.items():
            if not isinstance(result, dict) or not result.get("satisfied"):
                continue

            instantiable_context_ids.append(container_id)
            if result.get("depends_on") is not None:
                depends_on = result.get("depends_on")

            for device_id in result.get("matched_devices") or []:
                if device_id not in matched_devices:
                    matched_devices.append(device_id)
            for zone_id in result.get("zones") or []:
                if zone_id not in zones:
                    zones.append(zone_id)
            for space_id in result.get("spaces") or []:
                if space_id not in spaces:
                    spaces.append(space_id)

        timings["matched_evidence_aggregation_seconds"] = round(perf_counter() - evidence_start, 6)

        return {
            "task_id": task_id,
            "satisfied": bool(instantiable_context_ids or launched_context_ids),
            "dependency_chain": dependency_chain,
            "depends_on": depends_on,
            "matched_devices": matched_devices,
            "zones": zones,
            "spaces": spaces,
            "launched_context_ids": launched_context_ids,
            "not_launched_context_ids": not_launched_context_ids,
            "instantiable_context_ids": instantiable_context_ids,
            "not_launched_results": all_results_not_launched,
            "execution_time_seconds": timings,
        }
        
        
    def resolve_task_multiple_building(
        self,
        task_id: str,
        dependency_chain: Dict[str, List[str]],
        topology_entities: List[TopologyEntity],
        output_dir: str = "resolution_results",
    ) -> Dict[str, Any]:
        """
        Full resolution flow for a task and all its dependencies.

        Returns a dict with:
            {
                "task_id": str,
                "satisfied": bool,
                "depends_on": List[str] | None,   # direct task deps from chain
                "matched_devices": List[str],      # all device IDs across all tasks
                "zones": List[Dict],               # containers of matched devices
                "spaces": List[Dict],              # containers of those zones
            }
        """
        order = self._build_resolution_order(task_id, dependency_chain)
        satisfaction: Dict[str, bool] = {}

        # Collect all matched device IDs across every task in the chain
        all_device_ids: List[str] = []

        for tid, children in order:
            print(f"\n{'='*60}")
            print(f"Task: {tid}")

            # --- Check task requirements first ---
            if children:
                children_satisfied = all(
                    satisfaction.get(child, False) for child in children
                )
                if not children_satisfied:
                    unsatisfied = [c for c in children if not satisfaction.get(c, False)]
                    print(f"  [FAIL] task requirements not satisfied: {unsatisfied}")
                    satisfaction[tid] = False
                    continue
                else:
                    print(f"  [OK] task requirements satisfied: {children}")

            # --- Fetch and split requirements ---
            task_reqs, non_task_reqs = self._fetch_and_split_requirements(tid)

            if not non_task_reqs:
                print(f"  [OK] no non-task requirements")
                satisfaction[tid] = True
                # self._save_matched_entities(tid, [], satisfaction[tid], output_dir)
                continue

            # --- Parse specs ---
            parsed_specs = self._parse_requires_spec(non_task_reqs)

            spatial_clauses = None
            device_specs = []

            for clause_name, is_spatial, property_clauses in parsed_specs:
                if is_spatial:
                    spatial_clauses = property_clauses
                else:
                    device_specs.append((clause_name, property_clauses))

            # --- Match and filter ---
            all_matched_ids: List[str] = []
            spec_satisfaction: List[bool] = []
            for clause_name, property_clauses in device_specs:
                print(f"\n  Spec [{clause_name}]")

                candidates = self._match_entities(topology_entities, property_clauses)
                print(f"    → {len(candidates)} candidates before spatial filter")

                if not candidates:
                    spec_satisfaction.append(False) 
                    continue

                if spatial_clauses:
                    final_matched = [
                        e for e in candidates
                        if self._check_spatial_context(e, spatial_clauses)
                    ]
                else:
                    final_matched = candidates

                print(f"    → {len(final_matched)} entities after spatial filter")
                spec_satisfaction.append(len(final_matched) > 0)

                for e in final_matched:
                    if e.entity_id not in all_matched_ids:
                        all_matched_ids.append(e.entity_id)

            # --- Save and record satisfaction ---
            if device_specs:
                satisfaction[tid] = all(spec_satisfaction)
            elif children:
                satisfaction[tid] = all(satisfaction.get(child, False) for child in children)
            else:
                satisfaction[tid]=False

            # self._save_matched_entities(tid, all_matched_ids, satisfaction[tid], output_dir)

            if satisfaction[tid]:
                print(f"  [PASS] {tid} satisfied with {len(all_matched_ids)} entities")
                # Accumulate device IDs across all tasks
                for dev_id in all_matched_ids:
                    if dev_id not in all_device_ids:
                        all_device_ids.append(dev_id)
            else:
                print(f"  [FAIL] {tid} — no entities matched non-task requirements")

        # --- Build the final return payload ---
        root_satisfied = satisfaction.get(task_id, False)

        print(f"\n{'='*60}")
        print(f"Root task '{task_id}' satisfied: {root_satisfied}")

        if not root_satisfied:
            return {
                "task_id": task_id,
                "satisfied": False,
                "dependency_chain":dependency_chain,
                "depends_on": None,
                "matched_devices": [],
                "zones": [],
                "spaces": [],
            }

        # Direct task dependencies of the root from the chain
        depends_on = dependency_chain.get(task_id) or None

        # Collect zones and spaces for every matched device
        zones: List[str] = []
        spaces: List[str] = []
        seen_zone_ids: set = set()
        seen_space_ids: set = set()

        for device_id in all_device_ids:
            containers = self.gateway.get_device_container(device_id)

            for container in containers:
                container_id = container.get("container_id") or ""
                container_labels = container.get("container_labels") or []

                # Determine container type by checking its labels
                is_building = "Building" in container_labels
                is_space    = "Space" in container_labels
                is_zone     = "Zone" in container_labels

                # Building — skip, we already have building_id
                if is_building:
                    print(f"  [container] building skipped: {container_id}")
                    continue

                # Space — append directly to spaces
                elif is_space:
                    if container_id and container_id not in seen_space_ids:
                        seen_space_ids.add(container_id)
                        spaces.append(container_id)
                        print(f"  [container] space found: {container_id}")

                # Zone — append to zones then find its parent space
                elif is_zone:
                    if container_id and container_id not in seen_zone_ids:
                        seen_zone_ids.add(container_id)
                        zones.append(container_id)
                        print(f"  [container] zone found: {container_id}")

                        # Find the space this zone belongs to
                        space_results = self.gateway.get_zone_container_space(container_id)
                        for space in space_results:
                            space_id = space.get("space_id")
                            if space_id and space_id not in seen_space_ids:
                                seen_space_ids.add(space_id)
                                spaces.append(space_id)
                                print(f"  [container] space found via zone: {space_id}")

                else:
                    print(f"  [container] unknown label {container_labels} for: {container_id}, skipping")

        result = {
            "task_id": task_id,
            "satisfied": root_satisfied,
            "dependency_chain":dependency_chain,
            "depends_on": depends_on,
            "matched_devices": all_device_ids,
            "zones": zones,
            "spaces": spaces,
        }

        print(f"\nResult summary:")
        print(f"  depends_on      : {depends_on}")
        print(f"  matched_devices : {len(all_device_ids)}")
        print(f"  zones           : {len(zones)}")
        print(f"  spaces          : {len(spaces)}")

        return result   

    def resolve_task_multiple_building_NOT_USED(
        self,
        task_id: str,
        dependency_chain: Dict[str, List[str]],
        topology_entities: List[TopologyEntity],
        output_dir: str = "resolution_results",
    ) -> Dict[str, Any]:
        """
        Full resolution flow for a task and all its dependencies.

        Returns a dict with:
            {
                "task_id": str,
                "satisfied": bool,
                "depends_on": List[str] | None,   # direct task deps from chain
                "matched_devices": List[str],      # all device IDs across all tasks
                "zones": List[Dict],               # containers of matched devices
                "spaces": List[Dict],              # containers of those zones
            }
        """
        order = self._build_resolution_order(task_id, dependency_chain)
        satisfaction: Dict[str, bool] = {}

        # Collect all matched device IDs across every task in the chain
        all_device_ids: List[str] = []

        for tid, children in order:
            print(f"\n{'='*60}")
            print(f"Task: {tid}")

            # --- Check task requirements first ---
            if children:
                children_satisfied = all(
                    satisfaction.get(child, False) for child in children
                )
                if not children_satisfied:
                    unsatisfied = [c for c in children if not satisfaction.get(c, False)]
                    print(f"  [FAIL] task requirements not satisfied: {unsatisfied}")
                    satisfaction[tid] = False
                    continue
                else:
                    print(f"  [OK] task requirements satisfied: {children}")

            # --- Fetch and split requirements ---
            task_reqs, non_task_reqs = self._fetch_and_split_requirements(tid)

            if not non_task_reqs:
                print(f"  [OK] no non-task requirements")
                satisfaction[tid] = True
                # self._save_matched_entities(tid, [], satisfaction[tid], output_dir)
                continue

            # --- Parse specs ---
            parsed_specs = self._parse_requires_spec(non_task_reqs)

            spatial_clauses = None
            device_specs = []

            for clause_name, is_spatial, property_clauses in parsed_specs:
                if is_spatial:
                    spatial_clauses = property_clauses
                else:
                    device_specs.append((clause_name, property_clauses))

            # --- Match and filter ---
            all_matched_ids: List[str] = []

            for clause_name, property_clauses in device_specs:
                print(f"\n  Spec [{clause_name}]")

                candidates = self._match_entities(topology_entities, property_clauses)
                print(f"    → {len(candidates)} candidates before spatial filter")

                if not candidates:
                    continue

                if spatial_clauses:
                    final_matched = [
                        e for e in candidates
                        if self._check_spatial_context(e, spatial_clauses)
                    ]
                else:
                    final_matched = candidates

                print(f"    → {len(final_matched)} entities after spatial filter")

                for e in final_matched:
                    if e.entity_id not in all_matched_ids:
                        all_matched_ids.append(e.entity_id)

            # --- Save and record satisfaction ---
            if device_specs:
                satisfaction[tid] = len(all_matched_ids) > 0
            elif children:
                satisfaction[tid] = all(satisfaction.get(child, False) for child in children)
            else:
                satisfaction[tid]=False

            # self._save_matched_entities(tid, all_matched_ids, satisfaction[tid], output_dir)

            if satisfaction[tid]:
                print(f"  [PASS] {tid} satisfied with {len(all_matched_ids)} entities")
                # Accumulate device IDs across all tasks
                for dev_id in all_matched_ids:
                    if dev_id not in all_device_ids:
                        all_device_ids.append(dev_id)
            else:
                print(f"  [FAIL] {tid} — no entities matched non-task requirements")

        # --- Build the final return payload ---
        root_satisfied = satisfaction.get(task_id, False)

        print(f"\n{'='*60}")
        print(f"Root task '{task_id}' satisfied: {root_satisfied}")

        if not root_satisfied:
            return {
                "task_id": task_id,
                "satisfied": False,
                "dependency_chain":dependency_chain,
                "depends_on": None,
                "matched_devices": [],
                "zones": [],
                "spaces": [],
            }

        # Direct task dependencies of the root from the chain
        depends_on = dependency_chain.get(task_id) or None

        # Collect zones and spaces for every matched device
        zones: List[str] = []
        spaces: List[str] = []
        seen_zone_ids: set = set()
        seen_space_ids: set = set()

        for device_id in all_device_ids:
            containers = self.gateway.get_device_container(device_id)

            for container in containers:
                container_id = container.get("container_id") or ""
                container_labels = container.get("container_labels") or []

                # Determine container type by checking its labels
                is_building = "Building" in container_labels
                is_space    = "Space" in container_labels
                is_zone     = "Zone" in container_labels

                # Building — skip, we already have building_id
                if is_building:
                    print(f"  [container] building skipped: {container_id}")
                    continue

                # Space — append directly to spaces
                elif is_space:
                    if container_id and container_id not in seen_space_ids:
                        seen_space_ids.add(container_id)
                        spaces.append(container_id)
                        print(f"  [container] space found: {container_id}")

                # Zone — append to zones then find its parent space
                elif is_zone:
                    if container_id and container_id not in seen_zone_ids:
                        seen_zone_ids.add(container_id)
                        zones.append(container_id)
                        print(f"  [container] zone found: {container_id}")

                        # Find the space this zone belongs to
                        space_results = self.gateway.get_zone_container_space(container_id)
                        for space in space_results:
                            space_id = space.get("space_id")
                            if space_id and space_id not in seen_space_ids:
                                seen_space_ids.add(space_id)
                                spaces.append(space_id)
                                print(f"  [container] space found via zone: {space_id}")

                else:
                    print(f"  [container] unknown label {container_labels} for: {container_id}, skipping")

        result = {
            "task_id": task_id,
            "satisfied": root_satisfied,
            "dependency_chain":dependency_chain,
            "depends_on": depends_on,
            "matched_devices": all_device_ids,
            "zones": zones,
            "spaces": spaces,
        }

        print(f"\nResult summary:")
        print(f"  depends_on      : {depends_on}")
        print(f"  matched_devices : {len(all_device_ids)}")
        print(f"  zones           : {len(zones)}")
        print(f"  spaces          : {len(spaces)}")

        return result

        # # Collect zones for every matched device
        # zones: List[Dict[str, Any]] = []
        # seen_zone_ids: set = set()

        # for device_id in all_device_ids:
        #     containers = self.gateway.get_device_container(device_id)
        #     for container in containers:
        #         zone_id = container.get("container_id")
        #         if zone_id and zone_id not in seen_zone_ids:
        #             seen_zone_ids.add(zone_id)
        #             zones.append({
        #                 "zone_id": zone_id,
        #                 "zone_name": container.get("container_name"),
        #                 "labels": container.get("container_labels"),
        #                 "properties": container.get("container_properties"),
        #                 "device_id": device_id,
        #             })

        # # Collect spaces for every zone
        # spaces: List[Dict[str, Any]] = []
        # seen_space_ids: set = set()

        # for zone in zones:
        #     zone_id = zone.get("zone_id")
        #     if not zone_id:
        #         continue
        #     space_results = self.gateway.get_zone_container_space(zone_id)
        #     for space in space_results:
        #         space_id = space.get("space_id")
        #         if space_id and space_id not in seen_space_ids:
        #             seen_space_ids.add(space_id)
        #             spaces.append({
        #                 "space_id": space_id,
        #                 "space_name": space.get("space_name"),
        #                 "labels": space.get("space_labels"),
        #                 "properties": space.get("space_properties"),
        #                 "zone_id": zone_id,
        #             })

        # result = {
        #     "task_id": task_id,
        #     "satisfied": root_satisfied,
        #     "depends_on": depends_on,
        #     "matched_devices": all_device_ids,
        #     "zones": [x['zone_id'] for x in zones],
        #     "spaces": [x['space_id'] for x in spaces],
        # }

        # print(f"\nResult summary:")
        # print(f"  depends_on      : {depends_on}")
        # print(f"  matched_devices : {len(all_device_ids)}")
        # print(f"  zones           : {len(zones)}")
        # print(f"  spaces          : {len(spaces)}")

        # return result
    
    def resolve_task_single_building(   
        self,
        task_id: str,
        dependency_chain: Dict[str, List[str]],
        topology_entities: List[TopologyEntity],
        output_dir: str = "resolution_results",
    ) -> bool:
        """
        Full resolution flow for a task and all its dependencies.

        For each task in bottom-up order:
        1. Fetch and split requirements (Step 2)
        2. Parse requires spec from non-task requirements (Step 3)
        3. Match property clauses against topology entities (Step 4)
        4. Filter matched entities through spatial context check (Step 5)
        5. Save matched entity IDs to JSON file and return True/False (Step 6)

        A task is satisfied if:
        - ALL its task requirements (children) are satisfied AND
        - At least one entity satisfies its non-task requirements

        Returns True if the root task is satisfied, False otherwise.
        """
        order = self._build_resolution_order(task_id, dependency_chain)

        # Track satisfaction result for each task
        satisfaction: Dict[str, bool] = {}

        for tid, children in order:
            print(f"\n{'='*60}")
            print(f"Task: {tid}")

            # --- Check task requirements first ---
            # All children must be satisfied before we check non-task reqs
            if children:
                children_satisfied = all(
                    satisfaction.get(child, False) for child in children
                )
                if not children_satisfied:
                    unsatisfied = [c for c in children if not satisfaction.get(c, False)]
                    print(f"  [FAIL] task requirements not satisfied: {unsatisfied}")
                    satisfaction[tid] = False
                    continue
                else:
                    print(f"  [OK] task requirements satisfied: {children}")

            # --- Fetch and split requirements ---
            task_reqs, non_task_reqs = self._fetch_and_split_requirements(tid)

            if not non_task_reqs:
                # No non-task requirements to check — satisfied by default
                print(f"  [OK] no non-task requirements")
                satisfaction[tid] = True
                self._save_matched_entities(tid,[],satisfaction[tid], output_dir)
                continue

            # --- Parse specs (Step 3) ---
            parsed_specs = self._parse_requires_spec(non_task_reqs)

            # Separate spatial spec from device specs
            spatial_clauses = None
            device_specs = []

            for clause_name, is_spatial, property_clauses in parsed_specs:
                if is_spatial:
                    spatial_clauses = property_clauses
                else:
                    device_specs.append((clause_name, property_clauses))

            # --- Match and filter for each device spec (Steps 4 + 5) ---
            all_matched_ids: List[str] = []

            for clause_name, property_clauses in device_specs:
                print(f"\n  Spec [{clause_name}]")

                candidates = self._match_entities(topology_entities, property_clauses)
                print(f"    → {len(candidates)} candidates before spatial filter")

                if not candidates:
                    continue

                if spatial_clauses:
                    final_matched = [
                        e for e in candidates
                        if self._check_spatial_context(e, spatial_clauses)
                    ]
                else:
                    final_matched = candidates

                print(f"    → {len(final_matched)} entities after spatial filter")

                for e in final_matched:
                    if e.entity_id not in all_matched_ids:
                        all_matched_ids.append(e.entity_id)

            # --- Save and record satisfaction (Step 6) ---

            if device_specs:
                satisfaction[tid] = len(all_matched_ids) > 0
            else:
                if spatial_clauses:
                    satisfaction[tid] = True
            self._save_matched_entities(tid, all_matched_ids,satisfaction[tid], output_dir)
            

            if satisfaction[tid]:
                print(f"  [PASS] {tid} satisfied with {len(all_matched_ids)} entities")
            else:
                print(f"  [FAIL] {tid} — no entities matched non-task requirements")

        root_satisfied = satisfaction.get(task_id, False)
        print(f"\n{'='*60}")
        print(f"Root task '{task_id}' satisfied: {root_satisfied}")
        return root_satisfied
    
    def _append_matched_to_task_file(
        self,
        task_id: str,
        matched_entity_ids: List[str],
        task_files_dir: str = "Task_Discovery/all_task",
    ) -> None:
        """
        Finds the existing JSON file for task_id in task_files_dir,
        reads its content, and appends matched_entity_ids under a new
        'matched_entities' key inside the 'requires' dict.

        Only modifies the file if matched_entity_ids is non-empty.
        Does nothing if the file does not exist.

        Example — before:
            {
                "requires": {
                    "SpacialContext": {...},
                    "TotalLoad": {...}
                }
            }

        Example — after:
            {
                "requires": {
                    "SpacialContext": {...},
                    "TotalLoad": {...},
                    "matched_entities": [
                        "urn:ngsi-ld:Device:...:Sensor_Total-Load"
                    ]
                }
            }
        """
        if not matched_entity_ids:
            print(f"  [append] skipping — no matched entities for: {task_id}")
            return

        # Build the expected filename — task_id with colons replaced by underscores
        safe_name = task_id.replace(":", "_").replace("/", "_")
        filepath = os.path.join(task_files_dir, f"{safe_name}.json")

        if not os.path.exists(filepath):
            print(f"  [append] file not found, skipping: {filepath}")
            return

        # Read existing content
        with open(filepath, "r") as f:
            try:
                content = json.load(f)
            except (json.JSONDecodeError, ValueError):
                print(f"  [append] failed to parse existing file: {filepath}")
                return

        # Content is a list — find the entry whose taskId matches
        if isinstance(content, list):
            for entry in content:
                if entry.get("taskId") == task_id:
                    requires = entry.get("requires")
                    if isinstance(requires, dict):
                        requires["matched_entities"] = matched_entity_ids
                        print(f"  [append] added matched_entities to requires in: {filepath}")
                    else:
                        print(f"  [append] no 'requires' dict found in entry for: {task_id}")
                    break
            else:
                print(f"  [append] no entry with taskId '{task_id}' found in: {filepath}")

        # Content is a plain dict (single entry file)
        elif isinstance(content, dict):
            requires = content.get("requires")
            if isinstance(requires, dict):
                requires["matched_entities"] = matched_entity_ids
                print(f"  [append] added matched_entities to requires in: {filepath}")
            else:
                print(f"  [append] no 'requires' dict found in: {filepath}")

        else:
            print(f"  [append] unexpected file structure in: {filepath}")
            return

        # Write back the modified content
        with open(filepath, "w") as f:
            json.dump(content, f, indent=4)

        print(f"  [append] saved updated file: {filepath}")
    
    def _save_matched_entities(
        self,
        task_id: str,
        matched_entity_ids: List[str],
        satisfactory_status: bool,
        output_dir: str = "resolution_results",
        task_files_dir: str = "Task_Discovery/all_task"
    ) -> None:
        """
        Saves the matched entity IDs to a JSON file named after the task.

        Output example for task "A":
            resolution_results/A.json
            {
                "task_id": "A",
                "matched_entities": [
                    "urn:ngsi-ld:Device:Lisbon-FCUL-Library:...:Sensor_Total-Load",
                    "urn:ngsi-ld:Device:Lisbon-FCUL-Library:...:Sensor_Accumulated-Load"
                ],
                "satisfied": true
            }
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Sanitise task_id to make it a valid filename
        safe_name = task_id.replace(":", "_").replace("/", "_")
        filepath = os.path.join(output_dir, f"{safe_name}.json")

        payload = {
            "task_id": task_id,
            "matched_entities": matched_entity_ids,
            "satisfied": satisfactory_status,
        }

        with open(filepath, "w") as f:
            json.dump(payload, f, indent=2)

        print(f"  [saved] {filepath} → satisfied={payload['satisfied']}")

        self._append_matched_to_task_file(
        task_id=task_id,
        matched_entity_ids=matched_entity_ids,
        task_files_dir=task_files_dir )

    def _check_spatial_context(
        self,
        entity: TopologyEntity,
        spatial_property_clauses: Dict[str, Any],
    ) -> bool:
        """
        Calls get_device_container for the entity and checks that at least
        one container satisfies the spatial property clauses.

        spatial_property_clauses example (the nodeProperties of the
        SpacialContext requirement node):
            {
                'type': ['Building', 'Zone', 'Space'],
                'area': True
            }

        Matching rules:
        - 'type' is matched against container_labels (OR logic)
        - 'area' being True means the container must have a non-empty area
        - all other fields are matched against container_properties (OR logic)
        """
        containers = self.gateway.get_device_container(entity.entity_id)

        if not containers:
            print(f"    [spatial] no containers found for: {entity.entity_id}")
            return False

        accepted_types = []
        raw_type = spatial_property_clauses.get("type")
        if raw_type:
            accepted_types = (
                [t.lower() for t in raw_type]
                if isinstance(raw_type, list)
                else [str(raw_type).lower()]
            )

        require_area = bool(spatial_property_clauses.get("area", False))

        for container in containers:
            container_labels = [
                lbl.lower() for lbl in (container.get("container_labels") or [])
            ]
            container_props = container.get("container_properties") or {}

            # Type check — container label must be in accepted types
            if accepted_types:
                if not any(lbl in accepted_types for lbl in container_labels):
                    print(f"    [spatial] label mismatch for container of: {entity.entity_id} → {container_labels}")
                    continue

            # Area check — must exist and be non-empty
            if require_area:
                area_val = container_props.get("area")
                if not area_val or str(area_val).strip() in ("", "null", "None"):
                    print(f"    [spatial] no area for container of: {entity.entity_id}")
                    continue

            # This container satisfies the spatial spec
            print(f"    [spatial] PASS for: {entity.entity_id} → container: {container.get('container_id')}")
            return True

        return False
        
    def _parse_requires_spec(
        self,
        non_task_requirements: List[Dict[str, Any]],
    ) -> List[Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]]:
        """
        Extracts the matching spec directly from nodeProperties of each
        non-task requirement node.

        nodeProperties example:
            {
                'unit': ['KWH', 'KWT'],
                'category': ['Sensor'],
                'measuresProperty': ['Accumulated-Load', 'Total-Load'],
                'type': ['Device'],
                'signature': ['53d08b76...'],
                'key': 'TotalLoad'
            }

        Returns a list of (clause_name, spatial_spec, property_clauses) tuples:
            - clause_name     : value of the 'key' field e.g. "TotalLoad"
            - spatial_spec    : SpacialContext dict if present, else None
            - property_clauses: all fields except 'key', 'signature', 'SpacialContext'
        """
        IGNORED_KEYS = {"key", "signature"}

        parsed_specs = []

        for req in non_task_requirements:
            req_props = req.get("nodeProperties") or {}

            if not req_props:
                print(f"  [skip] empty nodeProperties for: {req.get('identifier')}")
                continue

            clause_name = req_props.get("key", req.get("identifier", "unknown"))

            spatial_spec = clause_name == 'SpacialContext'

            property_clauses = {
                k: v for k, v in req_props.items()
                if k not in IGNORED_KEYS
            }

            if not property_clauses:
                print(f"  [skip] no usable clauses for: {clause_name}")
                continue

            print(f"  [parsed] {clause_name}: spatial={spatial_spec}, clauses={list(property_clauses.keys())}")
            parsed_specs.append((clause_name, spatial_spec, property_clauses))

        return parsed_specs


    def _match_entities(
        self,
        topology_entities: List[TopologyEntity],
        property_clauses: Dict[str, Any],
    ) -> List[TopologyEntity]:
        """
        Returns topology entities that satisfy ALL clauses in property_clauses.

        Matching rules:
        - clause key "type" is matched against entity.labels
        - all other keys are matched against entity.properties
        - if the clause value is a list, matching is OR (any element matches)
        - all comparisons are case-insensitive
        - if a clause value is itself a list in the spec, any element matching
        the entity value is enough
        """
        matched = []

        for entity in topology_entities:
            if self._entity_matches_clauses(entity, property_clauses):
                matched.append(entity)

        return matched
    
    def _entity_matches_clauses(
        self,
        entity: TopologyEntity,
        property_clauses: Dict[str, Any],
    ) -> bool:
        """
        Returns True if the entity satisfies ALL clauses (AND logic across
        clauses, OR logic within a clause when the value is a list).
        """
        for field, expected in property_clauses.items():

            if field == "type":
                entity_values = [lbl.lower() for lbl in entity.labels]
            else:
                raw = entity.properties.get(field)
                if raw is None:
                    return False
                # entity property value may itself be a list
                if isinstance(raw, list):
                    entity_values = [str(v).lower() for v in raw]
                else:
                    entity_values = [str(raw).lower()]

            # spec value may be a list (OR) or a single value
            if isinstance(expected, list):
                accepted = [str(v).lower() for v in expected]
            else:
                accepted = [str(expected).lower()]

            if not any(ev in accepted for ev in entity_values):
                return False

        return True

    
    def _build_resolution_order(
        self,
        task_id: str,
        dependency_chain: Dict[str, List[str]],
        visited: Optional[Set[str]] = None,
    ) -> List[Tuple[str, List[str]]]:
        """
        Recursively walks the dependency chain from task_id and returns
        a list of (task_id, its_task_requirements) in bottom-up order.
        Children are always resolved before their parent.

        Example:
            chain = {
                "A01": ["A01-K:00001-V01", "A01-K:00002-V01"],
                "A01-K:00001-V01": ["A", "B"],
                "A01-K:00002-V01": ["C", "D"],
                "A": [], "B": [], "C": [], "D": []
            }
            _build_resolution_order("A01", chain)
            → [
                ("A",               []),
                ("B",               []),
                ("A01-K:00001-V01", ["A", "B"]),
                ("C",               []),
                ("D",               []),
                ("A01-K:00002-V01", ["C", "D"]),
                ("A01",             ["A01-K:00001-V01", "A01-K:00002-V01"]),
            ]
        """
        if visited is None:
            visited = set()

        if task_id in visited:
            return []
        visited.add(task_id)

        children = dependency_chain.get(task_id, [])

        result = []

        # Recurse into children first (bottom-up)
        for child in children:
            result.extend(self._build_resolution_order(child, dependency_chain, visited))

        # Append self after all children are resolved
        result.append((task_id, children))

        return result
    
    def _fetch_and_split_requirements(
        self,
        task_id: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Fetches all requirements for a task and splits them into:
        - task_requirements     : nodes where nodeType contains "Task"
        - non_task_requirements : everything else (Device, Zone, Space, etc.)

        Returns:
            (task_requirements, non_task_requirements)
        """
        all_requirements = self.gateway.get_task_requirements(task_id)

        task_requirements = [
            r for r in all_requirements
            if "Task" in (r.get("nodeType") or [])
        ]

        non_task_requirements = [
            r for r in all_requirements
            if "Task" not in (r.get("nodeType") or [])
        ]

        return task_requirements, non_task_requirements

    def _entity_matches_clauses(
        self,
        entity: TopologyEntity,
        clauses: Dict[str, Any],
    ) -> bool:
        """
        Every clause in the dict must be satisfied (AND across clauses).
        Within a clause, if the spec value is a list it is OR — any element
        matching the entity property is enough.

        Special case: "type" matches against entity.labels, not properties.
        """
        for field, expected in clauses.items():

            if field == "type":
                actual_values = [lbl.lower() for lbl in entity.labels]
            else:
                raw = entity.properties.get(field)
                if raw is None:
                    return False
                actual_values = [str(raw).lower()]

            if isinstance(expected, list):
                accepted = [str(v).lower() for v in expected]
            else:
                accepted = [str(expected).lower()]

            if not any(av in accepted for av in actual_values):
                return False

        return True


    def _entity_satisfies_spatial_context(
        self,
        device_id: str,
        spatial_spec: Dict[str, Any],
    ) -> bool:
        """
        Calls get_device_container and checks that at least one container
        satisfies the spatial spec:
        - "type": ["Building", "Zone", "Space"]  → container label must match (OR)
        - "area": True                            → container.area must be non-empty
        """
        containers = self.gateway.get_device_container(device_id)

        if not containers:
            return False

        accepted_types = []
        raw_type = spatial_spec.get("type")
        if raw_type:
            accepted_types = (
                [t.lower() for t in raw_type]
                if isinstance(raw_type, list)
                else [str(raw_type).lower()]
            )

        require_area = bool(spatial_spec.get("area", False))

        for container in containers:
            container_labels = [
                lbl.lower() for lbl in (container.get("container_labels") or [])
            ]
            container_props = container.get("container_properties") or {}

            # Type check (OR across accepted types)
            if accepted_types:
                if not any(lbl in accepted_types for lbl in container_labels):
                    continue

            # Area check
            if require_area:
                area_val = container_props.get("area")
                if not area_val or str(area_val).strip() in ("", "null", "None"):
                    continue

            # This container satisfies the spatial spec
            return True

        return False   

    def _collect_task_chain__(
        self,
        task_id: str,
        lineage: Tuple[str, ...],
        cache: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        cached = cache.get(task_id)
        if cached is not None:
            return cached

        if task_id in lineage:
            return {
                "task": {"task_id": task_id, "name": None, "labels": [], "properties": {}},
                "requirements": [],
                "direct_dependencies": [],
                "dependency_task_ids": [],
                "dependency_cycles": [list(lineage + (task_id,))],
            }

        task = self.gateway.get_generic_task(task_id)
        if not task:
            return None

        requirements = self.gateway.get_task_requirements_old(task_id)
        direct_dependencies = self.gateway.get_direct_task_dependencies(task_id)

        dependency_task_ids: Set[str] = set()
        dependency_cycles: List[List[str]] = []

        for requirement in requirements:
            for target in requirement.get("targets") or []:
                target_task_id = target.get("task_id")
                if not target_task_id:
                    continue
                dependency_task_ids.add(target_task_id)
                child = self._collect_task_chain(target_task_id, lineage + (task_id,), cache)
                if child:
                    dependency_cycles.extend(child.get("dependency_cycles") or [])

        for dependency in direct_dependencies:
            dep_task_id = dependency.get("dependency_task_id")
            if not dep_task_id:
                continue
            dependency_task_ids.add(dep_task_id)
            child = self._collect_task_chain(dep_task_id, lineage + (task_id,), cache)
            if child:
                dependency_cycles.extend(child.get("dependency_cycles") or [])

        payload = {
            "task": task,
            "requirements": requirements,
            "direct_dependencies": direct_dependencies,
            "dependency_task_ids": sorted(dependency_task_ids),
            "dependency_cycles": dependency_cycles,
        }
        cache[task_id] = payload
        return payload

    def _collect_task_chain(
        self,
        task_id: str,
        lineage: Tuple[str, ...],
        cache: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        cached = cache.get(task_id)
        if cached is not None:
            return cached

        if task_id in lineage:
            return {
                "task": {"task_id": task_id, "name": None, "labels": [], "properties": {}},
                "requirements": [],
                "direct_dependencies": [],
                "dependency_task_ids": [],
                "dependency_cycles": [list(lineage + (task_id,))],
                "dependency_chain": {},
            }

        task = self.gateway.get_generic_task(task_id)
        if not task:
            return None

        all_requirements = self.gateway.get_task_requirements(task_id)
        task_indirect_requirements = [x for x in all_requirements if x['nodeType'][0] == 'Task']
        task_direct_requirements = [x for x in all_requirements if x['nodeType'][0] != 'Task']

        dependency_task_ids: Set[str] = set()
        dependency_cycles: List[List[str]] = []
        dependency_chain: Dict[str, List[str]] = {}

        # Collect direct task dependency IDs for the current task_id
        direct_dep_ids: List[str] = []
        for dependency in task_indirect_requirements:
            dep_task_id = dependency.get("nodeProperties").get('taskId')
            if not dep_task_id:
                continue
            direct_dep_ids.append(dep_task_id)
            dependency_task_ids.add(dep_task_id)

        # Register current task's direct deps in the chain
        dependency_chain[task_id] = direct_dep_ids

        # Recurse into each dependency to collect their chains
        for dep_task_id in direct_dep_ids:
            child = self._collect_task_chain(dep_task_id, lineage + (task_id,), cache)
            if child:
                dependency_cycles.extend(child.get("dependency_cycles") or [])
                # Merge child's dependency_chain dict into ours (child entries won't overlap)
                dependency_chain.update(child.get("dependency_chain") or {})

        payload = {
            "task": task,
            "requirements": task_direct_requirements,
            "direct_dependencies": task_indirect_requirements,
            "dependency_task_ids": sorted(dependency_task_ids),
            "dependency_cycles": dependency_cycles,
            "dependency_chain": dependency_chain,
        }
        cache[task_id] = payload
        return payload

    def _collect_task_chain_old(
        self,
        task_id: str,
        lineage: Tuple[str, ...],
        cache: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        cached = cache.get(task_id)
        if cached is not None:
            return cached

        if task_id in lineage:
            return {
                "task": {"task_id": task_id, "name": None, "labels": [], "properties": {}},
                "requirements": [],
                "direct_dependencies": [],
                "dependency_task_ids": [],
                "dependency_cycles": [list(lineage + (task_id,))],
                "dependency_chain": [],
            }

        task = self.gateway.get_generic_task(task_id)
        if not task:
            return None

        all_requirements = self.gateway.get_task_requirements(task_id)
        task_indirect_requirements = [x for x in all_requirements if x['nodeType'][0]=='Task']
        task_direct_requirements=[x for x in all_requirements if x['nodeType'][0]!='Task']

        dependency_task_ids: Set[str] = set()
        dependency_cycles: List[List[str]] = []
        dependency_chain: List[str] = [task_id]

        for dependency in task_indirect_requirements:
            dep_task_id = dependency.get("nodeProperties").get('taskId')
            if not dep_task_id:
                continue
            dependency_task_ids.add(dep_task_id)
            child = self._collect_task_chain(dep_task_id, lineage + (task_id,), cache)
            if child:
                dependency_cycles.extend(child.get("dependency_cycles") or [])
                # Only add child chain entries not already present (avoids dupes from shared deps)
                for tid in child.get("dependency_chain") or []:
                    if tid not in dependency_chain:
                        dependency_chain.append(tid)
        payload = {
            "task": task,
            "requirements": task_direct_requirements,
            "direct_dependencies":task_indirect_requirements,
            "dependency_task_ids": sorted(dependency_task_ids),
            "dependency_cycles": dependency_cycles,
            "dependency_chain": dependency_chain,
        }
        cache[task_id] = payload
        return payload
    
    def _evaluate_task(
        self,
        task_id: str,
        topology_entities: List[TopologyEntity],
        chain_cache: Dict[str, Dict[str, Any]],
        resolution_cache: Dict[str, TaskResolution],
        lineage: Tuple[str, ...],
    ) -> TaskResolution:
        if task_id in resolution_cache:
            return resolution_cache[task_id]

        chain = chain_cache.get(task_id)
        if not chain:
            missing = TaskResolution(
                task_id=task_id,
                task_name=None,
                satisfied=False,
                reason="task_not_found",
            )
            resolution_cache[task_id] = missing
            return missing

        if task_id in lineage:
            cyclic = TaskResolution(
                task_id=task_id,
                task_name=(chain.get("task") or {}).get("name"),
                satisfied=False,
                dependency_cycles=[list(lineage + (task_id,))],
                reason="cyclic_dependency",
            )
            resolution_cache[task_id] = cyclic
            return cyclic

        requirement_resolutions: List[RequirementResolution] = []
        direct_dependency_resolutions: List[DirectDependencyResolution] = []
        dependency_chain: Dict[str, TaskResolution] = {}
        matched_entity_ids: Set[str] = set()
        task_satisfied = True

        for requirement in chain.get("requirements") or []:
            req_props = dict(requirement.get("requirement_properties") or {})
            target_matches: List[RequirementTargetMatch] = []
            requirement_entity_ids: Set[str] = set()
            satisfied_dependency_task_ids: Set[str] = set()

            for target in requirement.get("targets") or []:
                target_task_id = target.get("task_id")
                if target_task_id:
                    child_resolution = self._evaluate_task(
                        task_id=target_task_id,
                        topology_entities=topology_entities,
                        chain_cache=chain_cache,
                        resolution_cache=resolution_cache,
                        lineage=lineage + (task_id,),
                    )
                    dependency_chain[target_task_id] = child_resolution
                    if child_resolution.satisfied:
                        satisfied_dependency_task_ids.add(target_task_id)
                        requirement_entity_ids.update(child_resolution.matched_entity_ids)

                    target_matches.append(
                        RequirementTargetMatch(
                            target_node_id=target.get("id"),
                            target_task_id=target_task_id,
                            target_labels=list(target.get("labels") or []),
                            target_properties=dict(target.get("properties") or {}),
                            required_properties={},
                            satisfied=child_resolution.satisfied,
                            matched_entity_ids=list(child_resolution.matched_entity_ids),
                        )
                    )
                    continue

                required_properties = self._build_required_properties(
                    requirement_properties=req_props,
                    target_properties=dict(target.get("properties") or {}),
                )
                matched_entities = self._match_entities__(
                    topology_entities=topology_entities,
                    required_labels=list(target.get("labels") or []),
                    required_properties=required_properties,
                )
                entity_ids = sorted(entity.entity_id for entity in matched_entities)
                if entity_ids:
                    requirement_entity_ids.update(entity_ids)

                target_matches.append(
                    RequirementTargetMatch(
                        target_node_id=target.get("id"),
                        target_task_id=None,
                        target_labels=list(target.get("labels") or []),
                        target_properties=dict(target.get("properties") or {}),
                        required_properties=required_properties,
                        satisfied=bool(entity_ids),
                        matched_entity_ids=entity_ids,
                    )
                )

            requirement_satisfied = bool(target_matches) and all(match.satisfied for match in target_matches)
            if not requirement_satisfied:
                task_satisfied = False

            if requirement_satisfied:
                matched_entity_ids.update(requirement_entity_ids)
            requirement_resolutions.append(
                RequirementResolution(
                    requirement_eid=requirement.get("requirement_eid") or "",
                    requirement_id=requirement.get("requirement_id"),
                    satisfied=requirement_satisfied,
                    matched_entity_ids=sorted(requirement_entity_ids),
                    satisfied_dependency_task_ids=sorted(satisfied_dependency_task_ids),
                    target_matches=target_matches,
                    reason=None if requirement_satisfied else "requirement_not_satisfied",
                )
            )

        for dependency in chain.get("direct_dependencies") or []:
            dep_task_id = dependency.get("dependency_task_id")
            if not dep_task_id:
                continue

            child_resolution = self._evaluate_task(
                task_id=dep_task_id,
                topology_entities=topology_entities,
                chain_cache=chain_cache,
                resolution_cache=resolution_cache,
                lineage=lineage + (task_id,),
            )
            dependency_chain[dep_task_id] = child_resolution
            matched_entity_ids.update(child_resolution.matched_entity_ids)
            if not child_resolution.satisfied:
                task_satisfied = False

            direct_dependency_resolutions.append(
                DirectDependencyResolution(
                    dependency_task_id=dep_task_id,
                    satisfied=child_resolution.satisfied,
                    reason=None if child_resolution.satisfied else "dependency_not_satisfied",
                )
            )

        result = TaskResolution(
            task_id=task_id,
            task_name=(chain.get("task") or {}).get("name"),
            satisfied=task_satisfied,
            matched_entity_ids=sorted(matched_entity_ids),
            requirements=requirement_resolutions,
            direct_dependencies=direct_dependency_resolutions,
            dependency_chain=[
                dependency_chain[dep_task_id]
                for dep_task_id in sorted(dependency_chain)
            ],
            dependency_cycles=list(chain.get("dependency_cycles") or []),
            reason="all_requirements_satisfied" if task_satisfied else "requirements_not_satisfied",
        )
        resolution_cache[task_id] = result
        return result

    def _build_required_properties(
        self,
        requirement_properties: Dict[str, Any],
        target_properties: Dict[str, Any],
    ) -> Dict[str, Any]:
        required_properties: Dict[str, Any] = {}
        for key in self._MATCHABLE_PROPERTY_KEYS:
            value = target_properties.get(key)
            if value is not None:
                required_properties[key] = value

        for key in self._MATCHABLE_PROPERTY_KEYS:
            value = requirement_properties.get(key)
            if value is not None:
                required_properties[key] = value

        required_properties.pop("signature", None)
        return required_properties

    def _match_entities__(
        self,
        topology_entities: List[TopologyEntity],
        required_labels: List[str],
        required_properties: Dict[str, Any],
    ) -> List[TopologyEntity]:
        filtered_labels = [
            label
            for label in required_labels
            if label
            and label != self.schema.task_label
            and label != self.schema.requirement_label
        ]

        matches: List[TopologyEntity] = []
        for entity in topology_entities:
            if self.schema.task_label in entity.labels or self.schema.requirement_label in entity.labels:
                continue

            if filtered_labels and not any(label in entity.labels for label in filtered_labels):
                continue

            if not self._properties_match(entity.properties, required_properties):
                continue

            matches.append(entity)

        return matches

    def _properties_match(self, entity_properties: Dict[str, Any], required_properties: Dict[str, Any]) -> bool:
        for key, expected in required_properties.items():
            if key == "signature":
                continue

            actual = entity_properties.get(key)
            if isinstance(expected, list):
                if isinstance(actual, list):
                    if not any(value in expected for value in actual):
                        return False
                elif actual not in expected:
                    return False
                continue

            if isinstance(actual, list):
                if expected not in actual:
                    return False
                continue

            if actual != expected:
                return False

        return True

    def _build_task_payload(
        self,
        chain: Dict[str, Any],
        resolution: TaskResolution,
        chain_cache: Dict[str, Dict[str, Any]],
        resolution_cache: Dict[str, TaskResolution],
        lineage: Tuple[str, ...],
    ) -> Dict[str, Any]:
        task = dict((chain.get("task") or {}).get("properties") or {})
        task.pop("signature", None)
        current_task_id = str(task.get(self.schema.task_id_property) or resolution.task_id)

        if current_task_id in lineage:
            task["requires"] = {}
            task["dependencyCycle"] = list(lineage + (current_task_id,))
            return self._normalize_properties(task)

        requires: Dict[str, Any] = {}
        requirement_resolutions = {
            (item.requirement_id, item.requirement_eid): item
            for item in resolution.requirements
        }

        for requirement in chain.get("requirements") or []:
            requirement_key = self._requirement_key(requirement)
            if not requirement_key:
                continue

            req_props = dict(requirement.get("requirement_properties") or {})
            req_props.pop("key", None)
            req_props.pop("signature", None)
            normalized_props = self._normalize_properties(req_props)

            match = requirement_resolutions.get(
                (requirement.get("requirement_id"), requirement.get("requirement_eid") or "")
            )
            if match and match.matched_entity_ids:
                normalized_props["ids"] = list(match.matched_entity_ids)

            requires[requirement_key] = normalized_props

        task["requires"] = requires

        dependency_payloads: List[Dict[str, Any]] = []
        dependency_ids: Set[str] = set()
        for requirement in chain.get("requirements") or []:
            for target in requirement.get("targets") or []:
                target_task_id = target.get("task_id")
                if target_task_id:
                    dependency_ids.add(target_task_id)
        for dependency in chain.get("direct_dependencies") or []:
            dep_task_id = dependency.get("dependency_task_id")
            if dep_task_id:
                dependency_ids.add(dep_task_id)

        for dependency_task_id in sorted(dependency_ids):
            child_chain = chain_cache.get(dependency_task_id)
            child_resolution = resolution_cache.get(dependency_task_id)
            if not child_chain or not child_resolution:
                continue
            dependency_payloads.append(
                self._build_task_payload(
                    chain=child_chain,
                    resolution=child_resolution,
                    chain_cache=chain_cache,
                    resolution_cache=resolution_cache,
                    lineage=lineage + (current_task_id,),
                )
            )

        if dependency_payloads:
            task["dependsOn"] = dependency_payloads

        return self._normalize_properties(task)

    def _requirement_key(self, requirement: Dict[str, Any]) -> Optional[str]:
        props = requirement.get("requirement_properties") or {}
        key = props.get("key")
        if isinstance(key, list):
            return str(key[0]) if key else None
        return str(key) if key is not None else None

    def _normalize_properties(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        for key, value in payload.items():
            if key == "signature":
                continue
            normalized[key] = self._normalize_value(value)
        return normalized

    def _normalize_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return self._normalize_properties(value)
        if isinstance(value, list):
            if len(value) == 1 and not isinstance(value[0], (dict, list)):
                return value[0]
            return [self._normalize_value(item) for item in value]
        return value
