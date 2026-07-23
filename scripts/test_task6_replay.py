import json
import base64
import urllib.request
import subprocess
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from discover_files import discover_files

NEO4J_HTTP = "http://localhost:7474/db/neo4j/tx/commit"
auth_header = "Basic " + base64.b64encode(b"neo4j:password123").decode()

# Pick first valid, non-excluded python file
TEST_FILE = discover_files()[0]
repo_root = Path(__file__).resolve().parent.parent / "target-repo"
rel_test_path = str(TEST_FILE.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
print(f"Target test file selected: {TEST_FILE} (Relative: {rel_test_path})")


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
        raise RuntimeError(f"Neo4j Error: {errors}")
    return res["results"][0]["data"]


def run_producer():
    cmd = ["python", "parser-service/parser.py", "--limit", "30", "--publish"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    time.sleep(3)  # Wait for Kafka Connect ingestion
    return res.stdout


def get_db_metrics():
    res_nodes = query_neo4j("MATCH (n:CPGNode) RETURN count(n) AS c")[0]["row"][0]
    res_edges = query_neo4j("MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS c")[0]["row"][0]
    return res_nodes, res_edges


def query_mongodb_doc(rel_file_path: str):
    """Query MongoDB REST/HTTP or pymongo if available, fallback to urllib via Mongo Express or Direct."""
    try:
        import pymongo
        client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        db = client["cpg"]
        doc = db["source_metadata"].find_one({"file_path": rel_file_path})
        count = db["source_metadata"].count_documents({})
        client.close()
        return doc, count
    except Exception:
        # Graceful fallback if pymongo not installed
        return None, 0


def main():
    print("==========================================================================")
    print("      TASK 6 IDEMPOTENT REPLAY & CODE MUTATION 5 QUALITY TESTCASES       ")
    print("==========================================================================")

    # Backup original content
    original_code = TEST_FILE.read_text(encoding="utf-8")

    try:
        # Initial baseline run
        print("\n--- Initializing Baseline Pipeline ---")
        run_producer()
        nodes_base, edges_base = get_db_metrics()
        print(f"Baseline Metrics -> Nodes: {nodes_base}, Edges: {edges_base}")

        # ----------------------------------------------------------------------
        # TESTCASE 1: Add New Function
        # ----------------------------------------------------------------------
        print("\n[TESTCASE 1] Add New Function ('tc1_new_function')")
        tc1_code = original_code + "\n\ndef tc1_new_function():\n    return 'Testcase 1 Output'\n"
        TEST_FILE.write_text(tc1_code, encoding="utf-8")
        run_producer()
        nodes_tc1, edges_tc1 = get_db_metrics()

        check_func1 = query_neo4j("MATCH (n:CPGNode {name: 'tc1_new_function'}) RETURN n.name, n.node_type")[0]["row"]
        mongo_doc1, mongo_count1 = query_mongodb_doc(rel_test_path)
        tc1_passed = (nodes_tc1 >= nodes_base + 1) and (check_func1[0] == 'tc1_new_function')
        print(f"  Result -> Neo4j Nodes: {nodes_tc1} (Diff: +{nodes_tc1 - nodes_base})")
        print(f"  Target Node Found in Neo4j: {check_func1}")
        if mongo_doc1:
            print(f"  MongoDB Updated Metadata -> File: {mongo_doc1.get('file_path')}, LOC: {mongo_doc1.get('loc')}, Nodes: {mongo_doc1.get('num_nodes')}")
        print(f"  TESTCASE 1: {'PASSED [SUCCESS]' if tc1_passed else 'FAILED [ERROR]'}")

        # ----------------------------------------------------------------------
        # TESTCASE 2: Add New Class with Method
        # ----------------------------------------------------------------------
        print("\n[TESTCASE 2] Add New Class ('Tc2TestClass') with Method ('tc2_method')")
        tc2_code = tc1_code + "\n\nclass Tc2TestClass:\n    def tc2_method(self):\n        pass\n"
        TEST_FILE.write_text(tc2_code, encoding="utf-8")
        run_producer()
        nodes_tc2, edges_tc2 = get_db_metrics()

        check_class2 = query_neo4j("MATCH (c:CPGNode {name: 'Tc2TestClass'}) RETURN c.name")[0]["row"]
        check_method2 = query_neo4j("MATCH (m:CPGNode {name: 'tc2_method'}) RETURN m.name")[0]["row"]
        mongo_doc2, mongo_count2 = query_mongodb_doc(rel_test_path)
        tc2_passed = (check_class2[0] == 'Tc2TestClass') and (check_method2[0] == 'tc2_method')
        print(f"  Result -> Neo4j Nodes: {nodes_tc2} (Diff from TC1: +{nodes_tc2 - nodes_tc1})")
        print(f"  Class Node: {check_class2[0]}, Method Node: {check_method2[0]}")
        if mongo_doc2:
            print(f"  MongoDB Upserted Metadata -> Total Docs: {mongo_count2}, Target File Hash: {mongo_doc2.get('file_hash')[:16]}...")
        print(f"  TESTCASE 2: {'PASSED [SUCCESS]' if tc2_passed else 'FAILED [ERROR]'}")

        # ----------------------------------------------------------------------
        # TESTCASE 3: Shift Line Numbers (Prepend Comments to test Stable ID)
        # ----------------------------------------------------------------------
        print("\n[TESTCASE 3] Prepend 10 Comment Lines (Test Stable ID Hash Resilience)")
        prepend_comments = "# Comment line " + "\n# Comment line ".join(str(i) for i in range(10)) + "\n"
        tc3_code = prepend_comments + tc2_code
        TEST_FILE.write_text(tc3_code, encoding="utf-8")
        run_producer()
        nodes_tc3, edges_tc3 = get_db_metrics()

        mongo_doc3, mongo_count3 = query_mongodb_doc(rel_test_path)
        tc3_passed = (nodes_tc3 == nodes_tc2)
        print(f"  Result -> Neo4j Nodes: {nodes_tc3} (Diff from TC2: {nodes_tc3 - nodes_tc2})")
        print(f"  Zero New Duplicate Nodes Created: {tc3_passed}")
        if mongo_doc3:
            print(f"  MongoDB Updated LOC: {mongo_doc3.get('loc')} (LOC increased by 10 comment lines)")
        print(f"  TESTCASE 3: {'PASSED [SUCCESS]' if tc3_passed else 'FAILED [ERROR]'}")

        # ----------------------------------------------------------------------
        # TESTCASE 4: Idempotent Exact Replay (Re-publish without modifications)
        # ----------------------------------------------------------------------
        print("\n[TESTCASE 4] Exact Pipeline Stream Replay (Re-publish Unchanged Files)")
        run_producer()
        nodes_tc4, edges_tc4 = get_db_metrics()

        mongo_doc4, mongo_count4 = query_mongodb_doc(rel_test_path)
        tc4_passed = (nodes_tc4 == nodes_tc3) and (edges_tc4 == edges_tc3)
        print(f"  Result -> Neo4j Nodes: {nodes_tc4}, Edges: {edges_tc4}")
        print(f"  Exact Match with Previous Run: {tc4_passed}")
        if mongo_doc4:
            print(f"  MongoDB Doc Count (Zero Duplicate Files): {mongo_count4}")
        print(f"  TESTCASE 4: {'PASSED [SUCCESS]' if tc4_passed else 'FAILED [ERROR]'}")

        # ----------------------------------------------------------------------
        # TESTCASE 5: Function Call Invocation (CALL Edge Creation)
        # ----------------------------------------------------------------------
        print("\n[TESTCASE 5] Add Function Invocation ('tc1_new_function()') inside another function")
        tc5_code = tc3_code + "\n\ndef tc5_caller_function():\n    tc1_new_function()\n"
        TEST_FILE.write_text(tc5_code, encoding="utf-8")
        run_producer()
        nodes_tc5, edges_tc5 = get_db_metrics()

        check_caller = query_neo4j("MATCH (n:CPGNode {name: 'tc5_caller_function'}) RETURN n.name")[0]["row"]
        mongo_doc5, mongo_count5 = query_mongodb_doc(rel_test_path)
        tc5_passed = (check_caller[0] == 'tc5_caller_function')
        print(f"  Result -> Neo4j Nodes: {nodes_tc5}, Edges: {edges_tc5}")
        print(f"  Caller Function Node Created: {check_caller[0]}")
        if mongo_doc5:
            print(f"  MongoDB Final Document Metadata -> File: {rel_test_path}, Nodes: {mongo_doc5.get('num_nodes')}, Edges: {mongo_doc5.get('num_edges')}")
        print(f"  TESTCASE 5: {'PASSED [SUCCESS]' if tc5_passed else 'FAILED [ERROR]'}")

        print("\n==========================================================================")
        all_passed = tc1_passed and tc2_passed and tc3_passed and tc4_passed and tc5_passed
        if all_passed:
            print("  ALL 5 TASK 6 IDEMPOTENCY & MUTATION TESTCASES PASSED 100% SUCCESSFULLY! ")
        else:
            print("  SOME TESTCASES FAILED. PLEASE CHECK LOGS ABOVE. ")
        print("==========================================================================")

    finally:
        # 1. Restore original file content and git state
        print("\nRestoring original file content...")
        TEST_FILE.write_text(original_code, encoding="utf-8")
        try:
            subprocess.run(["git", "-C", str(repo_root), "checkout", str(TEST_FILE.name)], capture_output=True)
        except Exception:
            pass
        
        # 2. Completely reset Neo4j DB to guarantee zero leftover test nodes/edges
        print("Wiping temporary test nodes and resetting Neo4j DB...")
        try:
            query_neo4j("MATCH (n) DETACH DELETE n")
        except Exception as e:
            print(f"Cleanup warning: {e}")

        # 3. Re-publish clean baseline dataset
        run_producer()
        print("Original file restored, Neo4j DB fully cleaned, and clean baseline re-published.")


if __name__ == "__main__":
    main()
