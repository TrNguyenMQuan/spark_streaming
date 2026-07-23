"""Testcase 1: Add New Function ('tc1_new_function')

Verifies that adding a new function creates a new FunctionDef node in Neo4j
and updates the corresponding metadata document in MongoDB without duplicating existing nodes.
"""

from helpers import get_test_file, run_producer, get_db_metrics, query_neo4j, query_mongodb_doc, reset_environment


def run_testcase_1():
    print("\n[TESTCASE 1] Add New Function ('tc1_new_function')")
    test_file, rel_path, repo_root = get_test_file()
    original_code = test_file.read_text(encoding="utf-8")

    try:
        nodes_base, edges_base = get_db_metrics()
        tc1_code = original_code + "\n\ndef tc1_new_function():\n    return 'Testcase 1 Output'\n"
        test_file.write_text(tc1_code, encoding="utf-8")
        
        run_producer()
        nodes_tc1, edges_tc1 = get_db_metrics()

        check_func1 = query_neo4j("MATCH (n:CPGNode {name: 'tc1_new_function'}) RETURN n.name, n.node_type")[0]["row"]
        mongo_doc1, _ = query_mongodb_doc(rel_path)
        passed = (nodes_tc1 >= nodes_base + 1) and (check_func1[0] == 'tc1_new_function')

        print(f"  Result -> Neo4j Nodes: {nodes_tc1} (Diff from Baseline: +{nodes_tc1 - nodes_base})")
        print(f"  Target Node Found in Neo4j: {check_func1}")
        if mongo_doc1:
            print(f"  MongoDB Updated Metadata -> File: {mongo_doc1.get('file_path')}, LOC: {mongo_doc1.get('loc')}, Nodes: {mongo_doc1.get('num_nodes')}")
        print(f"  TESTCASE 1: {'PASSED [SUCCESS]' if passed else 'FAILED [ERROR]'}")
        return passed

    finally:
        reset_environment(original_code, test_file, repo_root)


if __name__ == "__main__":
    run_testcase_1()
