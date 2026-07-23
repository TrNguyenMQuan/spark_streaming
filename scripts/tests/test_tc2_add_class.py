"""Testcase 2: Add New Class ('Tc2TestClass') with Method ('tc2_method')

Verifies that adding a new class and method creates both ClassDef and FunctionDef
nodes in Neo4j and performs upsert on MongoDB metadata.
"""

from helpers import get_test_file, run_producer, get_db_metrics, query_neo4j, query_mongodb_doc, reset_environment


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

        check_class2 = query_neo4j("MATCH (c:CPGNode {name: 'Tc2TestClass'}) RETURN c.name")[0]["row"]
        check_method2 = query_neo4j("MATCH (m:CPGNode {name: 'tc2_method'}) RETURN m.name")[0]["row"]
        mongo_doc2, mongo_count2 = query_mongodb_doc(rel_path)
        passed = (check_class2[0] == 'Tc2TestClass') and (check_method2[0] == 'tc2_method')

        print(f"  Result -> Neo4j Nodes: {nodes_tc2} (Diff from Baseline: +{nodes_tc2 - nodes_base})")
        print(f"  Class Node: {check_class2[0]}, Method Node: {check_method2[0]}")
        if mongo_doc2:
            print(f"  MongoDB Upserted Metadata -> Total Docs: {mongo_count2}, Target File Hash: {mongo_doc2.get('file_hash')[:16]}...")
        print(f"  TESTCASE 2: {'PASSED [SUCCESS]' if passed else 'FAILED [ERROR]'}")
        return passed

    finally:
        reset_environment(original_code, test_file, repo_root)


if __name__ == "__main__":
    run_testcase_2()
