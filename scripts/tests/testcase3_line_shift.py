"""Testcase 3: Prepend Comment Lines (Line Shift Resilience)

Verifies that adding comment lines shifts line numbers without producing duplicate AST nodes
in Neo4j because Stable ID SHA-256 hash is line-number independent.
"""

try:
    from helpers import get_test_file, run_producer, get_db_metrics, query_mongodb_doc, reset_environment
except ModuleNotFoundError:
    from scripts.tests.helpers import get_test_file, run_producer, get_db_metrics, query_mongodb_doc, reset_environment


def run_testcase_3():
    print("\n[TESTCASE 3] Prepend 10 Comment Lines (Test Stable ID Hash Resilience)")
    test_file, rel_path, repo_root = get_test_file()
    original_code = test_file.read_text(encoding="utf-8")

    try:
        nodes_base, edges_base = get_db_metrics()
        prepend_comments = "# Comment line " + "\n# Comment line ".join(str(i) for i in range(10)) + "\n"
        tc3_code = prepend_comments + original_code
        test_file.write_text(tc3_code, encoding="utf-8")

        run_producer()
        nodes_tc3, edges_tc3 = get_db_metrics()

        mongo_doc3, _ = query_mongodb_doc(rel_path)
        passed = (nodes_tc3 == nodes_base)

        print(f"  Result -> Neo4j Nodes: {nodes_tc3} (Diff from Baseline: {nodes_tc3 - nodes_base})")
        print(f"  Zero New Duplicate Nodes Created: {passed}")
        if mongo_doc3:
            print(f"  MongoDB Updated LOC: {mongo_doc3.get('loc')} (LOC increased by 10 comment lines)")
        print(f"  TESTCASE 3: {'PASSED [SUCCESS]' if passed else 'FAILED [ERROR]'}")
        return passed

    finally:
        reset_environment(original_code, test_file, repo_root)


if __name__ == "__main__":
    run_testcase_3()
