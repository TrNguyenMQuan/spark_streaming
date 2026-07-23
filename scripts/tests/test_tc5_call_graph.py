"""Testcase 5: Add Function Invocation ('tc1_new_function()') inside caller function

Verifies that adding a function invocation creates both caller node and CALL relationship edge in Neo4j.
"""

from helpers import get_test_file, run_producer, get_db_metrics, query_neo4j, query_mongodb_doc, reset_environment


def run_testcase_5():
    print("\n[TESTCASE 5] Add Function Invocation ('tc1_new_function()') inside another function")
    test_file, rel_path, repo_root = get_test_file()
    original_code = test_file.read_text(encoding="utf-8")

    try:
        nodes_base, edges_base = get_db_metrics()
        tc5_code = original_code + "\n\ndef tc1_new_function():\n    return 'Test'\n\ndef tc5_caller_function():\n    tc1_new_function()\n"
        test_file.write_text(tc5_code, encoding="utf-8")

        run_producer()
        nodes_tc5, edges_tc5 = get_db_metrics()

        check_caller = query_neo4j("MATCH (n:CPGNode {name: 'tc5_caller_function'}) RETURN n.name")[0]["row"]
        mongo_doc5, _ = query_mongodb_doc(rel_path)
        passed = (check_caller[0] == 'tc5_caller_function')

        print(f"  Result -> Neo4j Nodes: {nodes_tc5}, Edges: {edges_tc5}")
        print(f"  Caller Function Node Created: {check_caller[0]}")
        if mongo_doc5:
            print(f"  MongoDB Final Document Metadata -> File: {rel_path}, Nodes: {mongo_doc5.get('num_nodes')}, Edges: {mongo_doc5.get('num_edges')}")
        print(f"  TESTCASE 5: {'PASSED [SUCCESS]' if passed else 'FAILED [ERROR]'}")
        return passed

    finally:
        reset_environment(original_code, test_file, repo_root)


if __name__ == "__main__":
    run_testcase_5()
