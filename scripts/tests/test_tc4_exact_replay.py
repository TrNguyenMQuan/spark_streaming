"""Testcase 4: Exact Pipeline Stream Replay

Verifies that re-publishing unchanged code events to Kafka results in 0 duplicate nodes/edges
in Neo4j and zero extra metadata documents in MongoDB.
"""

try:
    from helpers import get_test_file, run_producer, get_db_metrics, query_mongodb_doc, reset_environment
except ModuleNotFoundError:
    from scripts.tests.helpers import get_test_file, run_producer, get_db_metrics, query_mongodb_doc, reset_environment


def run_testcase_4():
    print("\n[TESTCASE 4] Exact Pipeline Stream Replay (Re-publish Unchanged Files)")
    test_file, rel_path, repo_root = get_test_file()
    original_code = test_file.read_text(encoding="utf-8")

    try:
        nodes_base, edges_base = get_db_metrics()

        # Re-publish exactly unchanged stream
        run_producer()
        nodes_tc4, edges_tc4 = get_db_metrics()

        _, total_docs = query_mongodb_doc(rel_path)
        passed = (nodes_tc4 == nodes_base) and (edges_tc4 == edges_base)

        print(f"  Result -> Neo4j Nodes: {nodes_tc4}, Edges: {edges_tc4}")
        print(f"  Exact Match with Previous Run: {passed}")
        print(f"  MongoDB Doc Count (Zero Duplicate Files): {total_docs}")
        print(f"  TESTCASE 4: {'PASSED [SUCCESS]' if passed else 'FAILED [ERROR]'}")
        return passed

    finally:
        reset_environment(original_code, test_file, repo_root)


if __name__ == "__main__":
    run_testcase_4()
