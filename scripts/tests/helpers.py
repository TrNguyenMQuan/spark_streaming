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
    res_edges = query_neo4j("MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS c")[0]["row"][0]
    return res_nodes, res_edges


def reset_environment(original_code, test_file, repo_root):
    """Restore original file content, wipe temporary Neo4j DB, and re-publish clean baseline dataset."""
    test_file.write_text(original_code, encoding="utf-8")
    try:
        subprocess.run(["git", "-C", str(repo_root), "checkout", str(test_file.name)], capture_output=True)
    except Exception:
        pass

    try:
        query_neo4j("MATCH (n) DETACH DELETE n")
    except Exception:
        pass

    run_producer()
