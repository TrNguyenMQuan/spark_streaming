"""Ground-Truth Accuracy Audit Script

Cross-checks 100% of ingested files, functions, and classes in Neo4j
against the raw Python source code files on GitHub / disk.
"""

import ast
import sys
from pathlib import Path

try:
    from helpers import query_neo4j, PROJECT_ROOT
except ModuleNotFoundError:
    from scripts.tests.helpers import query_neo4j, PROJECT_ROOT


def get_ast_ground_truth(file_path: Path):
    try:
        source = file_path.read_text(encoding="utf-8", errors="replace")
        tree = ast.parse(source)
    except Exception as exc:
        print(f"  ⚠️ Error parsing ground truth for {file_path.name}: {exc}")
        return {"functions": set(), "classes": set(), "total_nodes": 0}

    functions = set()
    classes = set()
    total_ast_nodes = 0

    for node in ast.walk(tree):
        total_ast_nodes += 1
        if isinstance(node, ast.FunctionDef):
            functions.add(node.name)
        elif isinstance(node, ast.ClassDef):
            classes.add(node.name)

    return {
        "functions": functions,
        "classes": classes,
        "total_nodes": total_ast_nodes
    }


def run_audit_accuracy():
    print("==========================================================================")
    print("        100% AUTOMATED FULL REPO ACCURACY AUDIT REPORT                   ")
    print("==========================================================================")

    cypher_files = "MATCH (n:CPGNode) RETURN DISTINCT n.file_path AS file_path ORDER BY file_path"
    rows = query_neo4j(cypher_files)
    ingested_files = [r["row"][0] for r in rows if r["row"][0]]

    print(f"Total source files ingested in Neo4j: {len(ingested_files)} files\n")

    repo_base = PROJECT_ROOT / "target-repo"

    total_files_checked = 0
    matched_files = 0
    total_funcs_gt = 0
    total_funcs_neo4j = 0
    total_classes_gt = 0
    total_classes_neo4j = 0
    discrepancies = []

    for file_rel in ingested_files:
        disk_path = repo_base / file_rel
        if not disk_path.exists():
            continue

        gt = get_ast_ground_truth(disk_path)

        cypher_file_detail = """
        MATCH (n:CPGNode {file_path: $file_path})
        RETURN n.node_type AS type, n.name AS name, labels(n) AS labels
        """
        detail_rows = query_neo4j(cypher_file_detail, {"file_path": file_rel})

        neo4j_funcs = set()
        neo4j_classes = set()

        for r in detail_rows:
            node_type = r["row"][0]
            node_name = r["row"][1]
            node_labels = r["row"][2]

            if node_name:
                if node_type == "FunctionDef" or "FunctionDef" in node_labels:
                    neo4j_funcs.add(node_name)
                elif node_type == "ClassDef" or "ClassDef" in node_labels:
                    neo4j_classes.add(node_name)

        total_files_checked += 1
        total_funcs_gt += len(gt["functions"])
        total_funcs_neo4j += len(neo4j_funcs)
        total_classes_gt += len(gt["classes"])
        total_classes_neo4j += len(neo4j_classes)

        func_match = (gt["functions"] == neo4j_funcs)
        class_match = (gt["classes"] == neo4j_classes)

        if func_match and class_match:
            matched_files += 1
        else:
            discrepancies.append({
                "file": file_rel,
                "missing_funcs": list(gt["functions"] - neo4j_funcs),
                "extra_funcs": list(neo4j_funcs - gt["functions"]),
                "missing_classes": list(gt["classes"] - neo4j_classes),
                "extra_classes": list(neo4j_classes - gt["classes"])
            })

    print("--------------------------------------------------------------------------")
    print(f"  1. Total files checked 1-to-1                : {total_files_checked}/{len(ingested_files)}")
    print(f"  2. Total files EXACT 100% MATCH              : {matched_files}/{total_files_checked} ({(matched_files/total_files_checked)*100:.1f}%)" if total_files_checked else 0)
    print(f"  3. Total Functions (FunctionDef) Ground Truth: {total_funcs_gt}")
    print(f"  4. Total Functions (FunctionDef) in Neo4j     : {total_funcs_neo4j} (Match Rate: {(total_funcs_neo4j/total_funcs_gt)*100 if total_funcs_gt else 100:.1f}%)")
    print(f"  5. Total Classes (ClassDef) Ground Truth    : {total_classes_gt}")
    print(f"  6. Total Classes (ClassDef) in Neo4j        : {total_classes_neo4j} (Match Rate: {(total_classes_neo4j/total_classes_gt)*100 if total_classes_gt else 100:.1f}%)")
    print("--------------------------------------------------------------------------")

    if not discrepancies:
        print("\nAUDIT SUCCESS: ALL INGESTED FILES IN NEO4J MATCH 100% WITH GITHUB SOURCE FILES!")
        return True
    else:
        print(f"\nDiscrepancies found in {len(discrepancies)} file(s):")
        for d in discrepancies[:5]:
            print(f"  - File: {d['file']}")
        return False


if __name__ == "__main__":
    run_audit_accuracy()
