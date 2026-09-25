from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List
import csv
from semantic_task_discovery.dashboard import build_dashboard_html, serve_dashboard
from semantic_task_discovery.discovery import SemanticTaskDiscoverer, SemanticTaskResolver
from semantic_task_discovery.neo4j_gateway import Neo4jConfig, Neo4jGateway
from semantic_task_discovery.reporting import TaskDiscoveryReporter
from semantic_task_discovery.schema import GraphSchema
from semantic_task_discovery.word_export import write_stage1_methodology_docx


def _write_json(path: str, payload: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _write_text(path: str, content: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        f.write(content)


def _architectural_component_timings(timings: Dict[str, Any]) -> Dict[str, Any]:
    """Return only architecture-level timing fields for the semantic report."""
    component_keys = [
        "building_context_extraction_seconds",
        "task_requirement_construction_seconds",
        "task_coverage_resolution_seconds",
        "semantic_matching_validation_seconds",
        "matched_evidence_aggregation_seconds",
        "task_manager_seconds",
        "total_execution_seconds",
    ]
    return {key: timings[key] for key in component_keys if key in timings}


def _task_manager_candidate_decision() -> Dict[str, Any]:
    """Report-only task-manager decision for a semantically instantiable context."""
    return {
        "status": "discovered_task_opportunity",
        "decision": "pending_approval",
        "canOperationalise": True,
        "operationalised": False,
        "kgUpdatePermitted": False,
        "reason": "All semantic requirements are satisfied, but the task manager has not approved KG instantiation.",
    }


def _task_manager_launched_decision() -> Dict[str, Any]:
    """Report-only task-manager decision for an already launched context."""
    return {
        "status": "already_operationalised",
        "decision": "no_action_required",
        "canOperationalise": False,
        "operationalised": True,
        "kgUpdatePermitted": False,
        "reason": "A task instance is already deployed in this context.",
    }


def _task_manager_not_instantiable_decision() -> Dict[str, Any]:
    """Report-only task-manager decision for a context with unmet requirements."""
    return {
        "status": "not_instantiable",
        "decision": "rejected_by_semantic_validation",
        "canOperationalise": False,
        "operationalised": False,
        "kgUpdatePermitted": False,
        "reason": "One or more mandatory semantic requirements are not satisfied.",
    }


def _build_task_manager_report(payload: Dict[str, Any]) -> Dict[str, Any]:
    launched_context_ids = payload.get("launched_context_ids", []) or []
    not_launched_results = payload.get("not_launched_results", {}) or {}
    instantiable_context_ids = payload.get("instantiable_context_ids", []) or []
    failed_context_ids = [
        context_id
        for context_id, result in not_launched_results.items()
        if not (isinstance(result, dict) and result.get("satisfied"))
    ]

    return {
        "component": "Task Manager",
        "mode": "report_only_no_kg_update",
        "purpose": "Separate discovered task opportunities from operational task instances.",
        "summary": {
            "alreadyOperationalisedContexts": len(launched_context_ids),
            "candidateContextsPendingApproval": len(instantiable_context_ids),
            "rejectedContexts": len(failed_context_ids),
        },
        "launchedDiscoveryDecisions": {
            context_id: _task_manager_launched_decision()
            for context_id in launched_context_ids
        },
        "notLaunchedDiscoveryDecisions": {
            context_id: (
                _task_manager_candidate_decision()
                if context_id in instantiable_context_ids
                else _task_manager_not_instantiable_decision()
            )
            for context_id in not_launched_results
        },
    }


def cmd_report(args: argparse.Namespace) -> None:
    schema = GraphSchema()
    gateway = Neo4jGateway(Neo4jConfig.from_env(), schema)
    payload: Dict[str, Any]
    try:
        reporter = TaskDiscoveryReporter(gateway)
        report = reporter.build_report(building_id=args.building_id)
        payload: Dict[str, Any] = report.to_dict()

        if args.output:
            _write_json(args.output, payload)
        if args.html_output:
            _write_text(args.html_output, build_dashboard_html(payload))
        if args.docx_output:
            write_stage1_methodology_docx(args.docx_output)
        elif not args.serve:
            print(json.dumps(payload, indent=2))
    finally:
        gateway.close()

    if args.serve:
        serve_dashboard(payload, host=args.host, port=args.port)


def cmd_discover(args: argparse.Namespace) -> None:
    schema = GraphSchema()
    gateway = Neo4jGateway(Neo4jConfig.from_env(), schema)
    try:
        discoverer = SemanticTaskDiscoverer(gateway=gateway, schema=schema, output_root=args.instances_dir)
        decisions = discoverer.discover_for_building(args.building_id)
        payload: Dict[str, List[Dict[str, Any]]] = {
            "building_id": args.building_id,
            "decisions": [d.__dict__ for d in decisions],
        }

        if args.output:
            _write_json(args.output, payload)
        else:
            print(json.dumps(payload, indent=2))
    finally:
        gateway.close()


def save_report_to_csv(report_dict, output_path: str = "task_report.csv") -> None:
    """
    Saves the report_dict as a single-row CSV file.
    Dictionary keys become column headers.
    List values are serialised as pipe-separated strings e.g. "id1|id2|id3"
    so the cell stays readable and parseable without nested quotes.
    """

    def serialise_value(val):
        if isinstance(val, list):
            # Each element might itself be a dict (zones/spaces payloads)
            parts = []
            for item in val:
                if isinstance(item, dict):
                    # Flatten dict to key=value pairs joined by semicolons
                    parts.append(";".join(f"{k}={v}" for k, v in item.items()))
                else:
                    parts.append(str(item))
            return ",".join(parts)
        if val is None:
            return ""
        return str(val)

    serialised = {k: serialise_value(v) for k, v in report_dict.items()}

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(serialised.keys()))
        writer.writeheader()
        writer.writerow(serialised)

    print(f"[saved] report written to: {output_path}")


def cmd_resolve_task(args: argparse.Namespace) -> None:
    schema = GraphSchema()
    gateway = Neo4jGateway(Neo4jConfig.from_env(), schema)
    building_ids=gateway.get_all_building_ids()
    building_ids=[x["building_id"] for x in building_ids]
    task_generic_info=gateway.get_generic_task(args.task_id)
    formatted_name = args.task_id.replace(":","_")
    save_dir = Path("csv_reports")
    save_dir.mkdir(parents=True, exist_ok=True)
    save_path = str(save_dir / f"{formatted_name}.csv")
    save_json_path = str(save_dir / f"{formatted_name}_new.json")



    try:
        resolver = SemanticTaskResolver(gateway=gateway, schema=schema)
        report_dict={}
        report_dict['taskid']=args.task_id
        report_dict['genericCategory']=task_generic_info['properties']["category"]
        report_dict['genericName']=task_generic_info['name']
        # Fill in default values prior to processing. The JSON report follows the
        # detailed semantic-discovery style used by
        # csv_reports_new_detailed/A11-A_FCC-V01_Lisbon-FCUL-Library_report.json.
        report_dict['requiresTaskIds']=False
        report_dict['dependency_chain']={}
        report_dict['executionTimeSeconds']={}
        report_dict['buildingIds']=[]
        for building_id in building_ids:
            # "urn:ngsi-ld:Building:Lisbon-FCUL-Library"
            # "urn:ngsi-ld:Building:Aalborg-Office"
            # urn:ngsi-ld:Building:Milan-MultiApartment-1
            # urn:ngsi-ld:Building:Milan-MultiApartment-1:Stair-1:Floor-1:Apartment-18
            if building_id != "urn:ngsi-ld:Building:Aalborg-Office":
                continue
            # if "Seixal" not in building_id:
            #     continue
            print(f"the building being processed is {building_id}")
            payload = resolver.resolve_for_building(building_id=building_id, task_id=args.task_id)

            task_manager_start = perf_counter()
            task_manager_report = _build_task_manager_report(payload)
            not_launched_discovery = {}
            for context_id, result in (payload.get("not_launched_results") or {}).items():
                if isinstance(result, dict) and result.get("satisfied"):
                    not_launched_discovery[context_id] = {
                        "Instantiable": True,
                        "taskManager": _task_manager_candidate_decision(),
                        "spaceIds": result.get("spaces", []),
                        "zoneIds": result.get("zones", []),
                        "deviceIds": result.get("matched_devices", []),
                    }
                else:
                    not_launched_discovery[context_id] = {}
            task_manager_seconds = round(perf_counter() - task_manager_start, 6)

            raw_timings = dict(payload.get("execution_time_seconds", {}))
            raw_timings["task_manager_seconds"] = task_manager_seconds
            raw_timings["total_execution_seconds"] = round(
                raw_timings.get("total_execution_seconds", 0) + task_manager_seconds,
                6,
            )
            component_timings = _architectural_component_timings(raw_timings)
            report_dict[building_id] = {
                "launched_discovery": payload.get("launched_context_ids", []),
                "not_launched_discovery": not_launched_discovery,
                "taskManager": task_manager_report,
                "executionTimeSeconds": component_timings,
            }
            report_dict['executionTimeSeconds'][building_id] = component_timings

            if payload.get("dependency_chain"):
                report_dict['dependency_chain'] = payload["dependency_chain"]
            if payload.get("depends_on"):
                report_dict['requiresTaskIds'] = True
            if payload.get("satisfied") and building_id not in report_dict['buildingIds']:
                report_dict['buildingIds'].append(building_id)
        
        semantic_report = {args.task_id: report_dict}
        with open(save_json_path,"w") as f:
            json.dump(semantic_report,f, indent=4)
        save_report_to_csv(report_dict, save_path)

        print("done")

        # if args.output:
        #     _write_json(args.output, payload)
        # else:
        #     print(json.dumps(payload, indent=2))
    finally:
        gateway.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Neo4j-based semantic task discovery and reporting for SATO tasks."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_report = subparsers.add_parser("report", help="Report instantiated vs non-instantiated tasks")
    p_report.add_argument("--building-id", help="Optional single building id. If omitted, all buildings are reported.")
    p_report.add_argument("--output", help="Optional JSON output file path.")
    p_report.add_argument("--html-output", help="Optional standalone HTML dashboard output path.")
    p_report.add_argument("--docx-output", help="Optional Word document path for the Stage 1 methodology explanation.")
    p_report.add_argument("--serve", action="store_true", help="Serve the read-only web dashboard locally.")
    p_report.add_argument("--host", default="127.0.0.1", help="Dashboard bind host when --serve is used.")
    p_report.add_argument("--port", type=int, default=8765, help="Dashboard port when --serve is used.")
    p_report.set_defaults(func=cmd_report)

    p_discover = subparsers.add_parser(
        "discover",
        help="For one building, discover and instantiate tasks with fully satisfied requirements.",
    )
    p_discover.add_argument("--building-id", required=True, help="Building id to evaluate")
    p_discover.add_argument(
        "--instances-dir",
        default="generated_task_instances",
        help="Folder where generated task-instance JSON files will be written.",
    )
    p_discover.add_argument("--output", help="Optional JSON output file path.")
    p_discover.set_defaults(func=cmd_discover)

    p_resolve = subparsers.add_parser(
        "resolve-task",
        help="Read-only resolution of one task against one building topology and dependency chain.",
    )
    # p_resolve.add_argument("--building-id", required=True, help="Building id to evaluate")
    p_resolve.add_argument("--task-id", required=True, help="Generic taskId to resolve")
    p_resolve.add_argument("--output", help="Optional JSON output file path.")
    p_resolve.set_defaults(func=cmd_resolve_task)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args(args=["resolve-task","--task-id","A11-A:FCC-V01","--output","new_reports/resolve.json"])
    args.func(args)


if __name__ == "__main__":
    main()
