import ast
import json
import base64
import urllib.request
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

NEO4J_HTTP = "http://localhost:7474/db/neo4j/tx/commit"
auth_header = "Basic " + base64.b64encode(b"neo4j:password123").decode()


def query_neo4j(cypher: str, params: dict = None):
    stmt = {"statement": cypher}
    if params:
        stmt["parameters"] = params
    payload = json.dumps({"statements": [stmt]}).encode("utf-8")
    req = urllib.request.Request(
        NEO4J_HTTP,
        data=payload,
        headers={"Content-Type": "application/json", "Authorization": auth_header}
    )
    with urllib.request.urlopen(req) as resp:
        res = json.loads(resp.read().decode())
    errors = res.get("errors", [])
    if errors:
        raise RuntimeError(f"Neo4j Query Error: {errors}")
    return res["results"][0]["data"]


def get_ast_ground_truth(file_path: Path):
    try:
        source = file_path.read_text(encoding="utf-8", errors="replace")
        tree = ast.parse(source)
    except Exception:
        return None  # Syntax error in file

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


def verify_all_ingested_files():
    print("==========================================================================")
    print("        100% AUTOMATED FULL REPO ACCURACY AUDIT REPORT                   ")
    print("==========================================================================")

    # Fetch distinct file_paths present in Neo4j DB
    cypher_files = "MATCH (n:CPGNode) RETURN DISTINCT n.file_path AS file_path ORDER BY file_path"
    rows = query_neo4j(cypher_files)
    ingested_files = [r["row"][0] for r in rows if r["row"][0]]

    print(f"Total source files ingested in Neo4j: {len(ingested_files)} files\n")

    total_files_checked = 0
    matched_files = 0
    total_funcs_gt = 0
    total_funcs_neo4j = 0
    total_classes_gt = 0
    total_classes_neo4j = 0

    discrepancies = []

    for file_rel in ingested_files:
        full_path = Path("target-repo") / file_rel if not file_rel.startswith("target-repo") else Path(file_rel)
        if not full_path.exists():
            full_path = Path(file_rel)

        if not full_path.exists():
            print(f"File not found on disk: {file_rel}")
            continue

        gt = get_ast_ground_truth(full_path)
        if not gt:
            continue

        # Query Neo4j for this specific file
        cypher_file_detail = """
        MATCH (n:CPGNode {file_path: $file_path})
        RETURN count(n) AS neo4j_nodes,
               collect(CASE WHEN n.node_type = 'FunctionDef' THEN n.name END) AS neo4j_funcs,
               collect(CASE WHEN n.node_type = 'ClassDef' THEN n.name END) AS neo4j_classes
        """
        detail = query_neo4j(cypher_file_detail, {"file_path": file_rel})[0]["row"]
        neo4j_nodes = detail[0]
        neo4j_funcs = set(filter(None, detail[1]))
        neo4j_classes = set(filter(None, detail[2]))

        total_files_checked += 1
        total_funcs_gt += len(gt["functions"])
        total_funcs_neo4j += len(neo4j_funcs)
        total_classes_gt += len(gt["classes"])
        total_classes_neo4j += len(neo4j_classes)

        # Compare functions and classes 1-1
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
    print(f"  2. Total files EXACT 100% MATCH              : {matched_files}/{total_files_checked} ({(matched_files/total_files_checked)*100:.1f}%)")
    print(f"  3. Total Functions (FunctionDef) Ground Truth: {total_funcs_gt}")
    print(f"  4. Total Functions (FunctionDef) in Neo4j     : {total_funcs_neo4j} (Match Rate: {(total_funcs_neo4j/total_funcs_gt)*100 if total_funcs_gt else 100:.1f}%)")
    print(f"  5. Total Classes (ClassDef) Ground Truth    : {total_classes_gt}")
    print(f"  6. Total Classes (ClassDef) in Neo4j        : {total_classes_neo4j} (Match Rate: {(total_classes_neo4j/total_classes_gt)*100 if total_classes_gt else 100:.1f}%)")
    print("--------------------------------------------------------------------------")

    if not discrepancies:
        print("\nAUDIT SUCCESS: ALL INGESTED FILES IN NEO4J MATCH 100% WITH GITHUB SOURCE FILES!")
    else:
        print(f"\nDiscrepancies found in {len(discrepancies)} file(s):")
        for d in discrepancies[:5]:
            print(f"  - File: {d['file']}")
            if d['missing_funcs']: print(f"    Missing funcs: {d['missing_funcs']}")
            if d['missing_classes']: print(f"    Missing classes: {d['missing_classes']}")


if __name__ == "__main__":
    verify_all_ingested_files()
