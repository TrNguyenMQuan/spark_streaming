import json
import base64
import urllib.request
import subprocess
import time
import sys
from pathlib import Path

# Add parser-service to sys.path for discover_files import
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "parser-service"))

from discover_files import discover_files

NEO4J_HTTP = "http://localhost:7474/db/neo4j/tx/commit"
auth_header = "Basic " + base64.b64encode(b"neo4j:password123").decode()


def get_test_file():
    """Select the first valid Python file in target-repo for mutation testing."""
    test_file = discover_files()[0]
    repo_root = PROJECT_ROOT / "target-repo"
    rel_path = str(test_file.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
    return test_file, rel_path, repo_root


def query_neo4j(cypher: str, params: dict = None):
    """Execute a Cypher query on Neo4j HTTP TX Commit endpoint."""
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


def query_mongodb_doc(rel_file_path: str):
    """Query MongoDB for source metadata of a given relative file path."""
    try:
        import pymongo
        client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        db = client["cpg"]
        doc = db["source_metadata"].find_one({"file_path": rel_file_path})
        count = db["source_metadata"].count_documents({})
        client.close()
        return doc, count
    except Exception:
        return None, 0


def run_producer(limit=30):
    """Run Producer parser.py to publish code events to Kafka topics."""
    cmd = ["python", "parser-service/parser.py", "--limit", str(limit), "--publish"]
    res = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True)
    time.sleep(3)  # Wait for Kafka Connect / Spark ingestion
    return res.stdout


def get_db_metrics():
    """Retrieve total count of nodes and edges in Neo4j."""
    res_nodes = query_neo4j("MATCH (n:CPGNode) RETURN count(n) AS c")[0]["row"][0]
    res_edges = query_neo4j("MATCH ()-[r]->() RETURN count(r) AS c")[0]["row"][0]
    return res_nodes, res_edges


def reset_environment(original_code=None, test_file=None, repo_root=None):
    """Restore original file content, wipe temporary Neo4j DB, and re-publish clean baseline dataset."""
    if "--no-teardown" in sys.argv:
        print("\n  [NO-TEARDOWN] Skipped environment cleanup. Mutated data kept in Neo4j & MongoDB for evidence screenshots.")
        print("  Run 'python scripts/tests/helpers.py --reset' whenever you want to restore baseline.\n")
        return

    if "--pause" in sys.argv:
        print("\n==========================================================================")
        print("  [PAUSE FOR SCREENSHOT EVIDENCE]")
        print("  Mutated testcase data is currently LIVE in Neo4j (http://localhost:7474) & MongoDB (http://localhost:8081)!")
        print("  --> Take your UI evidence screenshots now.")
        print("  --> Press [ENTER] key in this terminal when finished to restore baseline environment...")
        print("==========================================================================")
        try:
            input()
        except KeyboardInterrupt:
            pass

    if test_file and original_code:
        test_file.write_text(original_code, encoding="utf-8")

    if repo_root:
        try:
            subprocess.run(["git", "-C", str(repo_root), "checkout", "."], capture_output=True)
        except Exception:
            pass

    try:
        query_neo4j("MATCH (n) DETACH DELETE n")
    except Exception:
        pass

    try:
        import pymongo
        client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        client["cpg"]["source_metadata"].delete_many({})
        client.close()
    except Exception:
        pass

    run_producer()
    print("  [RESET COMPLETE] Environment successfully restored to clean baseline dataset.")


if __name__ == "__main__":
    if "--reset" in sys.argv:
        print("Restoring baseline environment...")
        test_file, rel_path, repo_root = get_test_file()
        reset_environment(repo_root=repo_root)
