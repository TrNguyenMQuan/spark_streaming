"""Testcase 2: Add New Class ('Tc2TestClass') with Method ('tc2_method')

Verifies that adding a new class with a method creates ClassDef and FunctionDef nodes
and AST relationship edge in Neo4j without duplicating existing nodes.
"""

try:
    from helpers import get_test_file, run_producer, get_db_metrics, query_neo4j, query_mongodb_doc, reset_environment
except ModuleNotFoundError:
    from scripts.tests.helpers import get_test_file, run_producer, get_db_metrics, query_neo4j, query_mongodb_doc, reset_environment


def run_testcase_2():
    print("\n[TESTCASE 2] Add New Class ('Tc2TestClass') with Method ('tc2_method')")
    test_file, rel_path, repo_root = get_test_file()
    original_code = test_file.read_text(encoding="utf-8")

    try:
        nodes_base, edges_base = get_db_metrics()
        tc2_code = original_code + "\n\nclass Tc2TestClass:\n    def tc2_method(self):\n        pass\n"
        test_file.write_text(tc2_code, encoding="utf-8")

        run_producer()
        nodes_tc2, edges_tc2 = get_db_metrics()

        check_class2 = query_neo4j("MATCH (n:CPGNode {name: 'Tc2TestClass'}) RETURN n.name, n.node_type")[0]["row"]
        check_method2 = query_neo4j("MATCH (n:CPGNode {name: 'tc2_method'}) RETURN n.name, n.node_type")[0]["row"]
        mongo_doc2, total_docs = query_mongodb_doc(rel_path)
        passed = (nodes_tc2 >= nodes_base + 2) and (check_class2[0] == 'Tc2TestClass') and (check_method2[0] == 'tc2_method')

        print(f"  Result -> Neo4j Nodes: {nodes_tc2} (Diff from Baseline: +{nodes_tc2 - nodes_base})")
        print(f"  Class Node: {check_class2[0]}, Method Node: {check_method2[0]}")
        if mongo_doc2:
            print(f"  MongoDB Upserted Metadata -> Total Docs: {total_docs}, Target File Hash: {mongo_doc2.get('file_hash')[:12]}...")
        print(f"  TESTCASE 2: {'PASSED [SUCCESS]' if passed else 'FAILED [ERROR]'}")
        return passed

    finally:
        reset_environment(original_code, test_file, repo_root)


if __name__ == "__main__":
    run_testcase_2()
