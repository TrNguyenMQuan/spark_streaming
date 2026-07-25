import json
import time
import urllib.request
import urllib.error
import base64
import zipfile
import io
import os
from pathlib import Path

NEO4J_HTTP = "http://localhost:7474"
CONNECT_REST = "http://localhost:8083/connectors"
NEO4J_AUTH = ("neo4j", "password123")
PLUGIN_URL = "https://github.com/neo4j/neo4j-kafka-connector/releases/download/5.5.0/neo4j-kafka-connect-5.5.0.zip"


def ensure_plugin_downloaded(root_dir: Path):
    plugins_dir = root_dir / "plugins"
    plugin_path = plugins_dir / "neo4j-kafka-connect-5.5.0"
    if not plugin_path.exists():
        print("Neo4j Kafka Connector plugin not found. Downloading...")
        plugins_dir.mkdir(exist_ok=True)
        req = urllib.request.Request(PLUGIN_URL, headers={"User-Agent": "Python"})
        with urllib.request.urlopen(req) as resp:
            content = resp.read()
        print("Extracting plugin to plugins/...")
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            z.extractall(plugins_dir)
        print("Plugin extracted successfully. (Restart kafka-connect if container is already running)")


def wait_for_service(url, name, max_retries=70, delay=3):
    print(f"Waiting for {name} at {url}...")
    for i in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Python"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status in (200, 401):
                    print(f"{name} is ready!")
                    return True
        except urllib.error.HTTPError as e:
            if e.code in (200, 401):
                print(f"{name} is ready!")
                return True
        except Exception:
            pass
        time.sleep(delay)
    raise RuntimeError(f"Timed out waiting for {name}")


def apply_neo4j_constraints():
    print("Applying Neo4j Cypher unique constraints and relationship indexes...")
    url = "http://localhost:7474/db/neo4j/tx/commit"
    cypher_node_constraint = (
        "CREATE CONSTRAINT unique_cpg_node_id IF NOT EXISTS FOR (n:CPGNode) REQUIRE n.node_id IS UNIQUE"
    )
    edge_types = ["AST", "CFG", "DFG", "CALL"]
    statements = [{"statement": cypher_node_constraint}]
    for et in edge_types:
        statements.append({
            "statement": f"CREATE INDEX {et.lower()}_edge_id_idx IF NOT EXISTS FOR ()-[r:{et}]-() ON (r.edge_id)"
        })

    payload = json.dumps({"statements": statements}).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    auth_str = base64.b64encode(f"{NEO4J_AUTH[0]}:{NEO4J_AUTH[1]}".encode()).decode()
    req.add_header("Authorization", f"Basic {auth_str}")

    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            errors = data.get("errors", [])
            if errors:
                print(f"️ Neo4j Cypher error: {errors}")
            else:
                print("Neo4j constraints & relationship indexes applied successfully.")
    except Exception as e:
        print(f"️ Error applying Neo4j constraints: {e}")


def register_connector(config_path: Path):
    with open(config_path, "r", encoding="utf-8") as f:
        connector_data = json.load(f)

    connector_name = connector_data["name"]
    print(f"Registering connector '{connector_name}'...")

    # Delete existing connector if it exists
    delete_req = urllib.request.Request(f"{CONNECT_REST}/{connector_name}", method="DELETE")
    try:
        with urllib.request.urlopen(delete_req):
            print(f"  Deleted previous instance of '{connector_name}'")
            time.sleep(1)
    except urllib.error.HTTPError:
        pass  # Was not registered yet

    # POST new config
    payload = json.dumps(connector_data).encode("utf-8")
    post_req = urllib.request.Request(CONNECT_REST, data=payload, headers={"Content-Type": "application/json"})

    try:
        with urllib.request.urlopen(post_req) as resp:
            res = json.loads(resp.read().decode())
            print(f"Connector '{connector_name}' registered successfully.")
    except urllib.error.HTTPError as e:
        print(f"️ Failed to register '{connector_name}': {e.read().decode()}")


def check_connector_status():
    print("\nChecking connector statuses:")
    time.sleep(2)
    req = urllib.request.Request(CONNECT_REST)
    with urllib.request.urlopen(req) as resp:
        connectors = json.loads(resp.read().decode())

    for c in connectors:
        status_req = urllib.request.Request(f"{CONNECT_REST}/{c}/status")
        with urllib.request.urlopen(status_req) as status_resp:
            st = json.loads(status_resp.read().decode())
            connector_state = st.get("connector", {}).get("state", "UNKNOWN")
            tasks = st.get("tasks", [])
            task_states = [t.get("state") for t in tasks]
            print(f"  - Connector '{c}': state={connector_state}, tasks={task_states}")


def main():
    root_dir = Path(__file__).resolve().parent.parent.parent
    ensure_plugin_downloaded(root_dir)

    wait_for_service(NEO4J_HTTP, "Neo4j")
    wait_for_service(CONNECT_REST, "Kafka Connect")

    apply_neo4j_constraints()

    nodes_config = root_dir / "neo4j" / "connectors" / "sink-nodes.json"
    edges_config = root_dir / "neo4j" / "connectors" / "sink-edges.json"

    register_connector(nodes_config)
    register_connector(edges_config)

    check_connector_status()


if __name__ == "__main__":
    main()
