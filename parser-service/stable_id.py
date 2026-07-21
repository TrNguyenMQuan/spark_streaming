# stable identifier use for requirement about incremental

import hashlib

ID_LENGTH = 24

def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:ID_LENGTH]

def make_node_id(file_path: str, qualified_scope: str, node_type: str, sibling_index: int) -> str:
    key = f"{file_path}|{qualified_scope}|{node_type}|{sibling_index}"
    return _sha(key)

def make_edge_id(egde_type: str, source_node_id: str, target_node_id: str,
                    dfg_variable: str | None = None) -> str:
    key = f"{egde_type}|{source_node_id}|{target_node_id}|{dfg_variable}"
    return _sha(key)

if __name__ == "__main__":
    n1 = make_node_id("src/bert.py", "Model.forward", "FunctionDef", 0)
    n2 = make_node_id("src/bert.py", "Model.forward", "If", 0)
    print("node_id (FunctionDef):", n1)
    print("node_id (If #0)      :", n2)
    print("edge_id (AST)        :", make_edge_id("AST", n1, n2))
    print("edge_id (DFG on 'b') :", make_edge_id("DFG", n1, n2, "b"))

    # Kiểm chứng ổn định: gọi lại y hệt -> ID KHÔNG đổi
    assert n1 == make_node_id("src/bert.py", "Model.forward", "FunctionDef", 0)
    print("OK: cùng input -> cùng ID (idempotent).")