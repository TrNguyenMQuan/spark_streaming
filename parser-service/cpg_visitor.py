# Traversal ast 1 python file create node event

import ast
import json
import sys
import hashlib
from datetime import datetime, timezone
from collections import Counter
from stable_id import make_node_id, make_edge_id

# Concern node
INTERESTING = {
    "Module",
    "FunctionDef", "AsyncFunctionDef", "ClassDef",
    "If", "For", "AsyncFor", "While", "Try", "With", "AsyncWith",
    "Assign", "AugAssign", "AnnAssign", "Return",
    "Call", "Import", "ImportFrom",
}

# Node create new scope
SCOPE_TYPES = {"FunctionDef", "AsyncFunctionDef", "ClassDef"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class CPGVisitor(ast.NodeVisitor):
    def __init__(self, file_path: str, repo_commit: str, repo: str | None = None):
        self.file_path = file_path
        self.repo_commit = repo_commit
        self.repo = repo
        self.scope_stack: list[str] = []       # name of scope
        self.counters: dict[tuple, int] = {}   # (scope, node_type) -> index sibling next
        self.nodes: list[dict] = []            # result: node list
        self.edges: list[dict] = []            # result: edges list
        self.node_id_stack: list[str] = []     # parent node
        self.id_map: dict[int, str] = {}       # id(ast_node) -> node_id (build CFG)


    def _scope_str(self) -> str:
        return ".".join(self.scope_stack)

    def _next_index(self, node_type: str) -> int:
        key = (self._scope_str(), node_type)
        idx = self.counters.get(key, 0)
        self.counters[key] = idx + 1
        return idx

    def _emit(self, node, node_type: str) -> str:
        scope = self._scope_str()
        idx = self._next_index(node_type)

        line_start = getattr(node, "lineno", 0)
        line_end = getattr(node, "end_lineno", None)
        if line_end is None:
            line_end = line_start

        event = {
            "schema_version": "v1",
            "event_timestamp": _now_iso(),
            "node_id": make_node_id(self.file_path, scope, node_type, idx),
            "node_type": node_type,
            "name": getattr(node, "name", None),   # only FunctionDef/ClassDef have .name
            "file_path": self.file_path,
            "line_start": line_start,
            "line_end": line_end,
            "col_start": getattr(node, "col_offset", None),
            "col_end": getattr(node, "end_col_offset", None),
            "repo_commit": self.repo_commit,
        }
        if self.repo:
            event["repo"] = self.repo
        self.nodes.append(event)
        self.id_map[id(node)] = event["node_id"]
        return event["node_id"]

    def _emit_ast_edge(self, source_id: str, target_id: str) -> None:
        # Edge AST: parent (source) -> child (target)
        self.edges.append({
            "schema_version": "v1",
            "event_timestamp": _now_iso(),
            "edge_id": make_edge_id("AST", source_id, target_id),
            "edge_type": "AST",
            "source_node_id": source_id,
            "target_node_id": target_id,
            "dfg_variable": None,
            "file_path": self.file_path,
            "repo_commit": self.repo_commit,
        })

    def _emit_cfg_edge(self, source_id: str, target_id: str) -> None:
        """Cạnh CFG: source thực thi xong -> target."""
        self.edges.append({
            "schema_version": "v1",
            "event_timestamp": _now_iso(),
            "edge_id": make_edge_id("CFG", source_id, target_id),
            "edge_type": "CFG",
            "source_node_id": source_id,
            "target_node_id": target_id,
            "dfg_variable": None,
            "file_path": self.file_path,
            "repo_commit": self.repo_commit,
        })

    def _emitted_stmts(self, node, field):
        stmts = getattr(node, field, [])
        if not isinstance(stmts, list):   # IfExp/Lambda có .body là 1 biểu thức, không phải list
            return []
        return [s for s in stmts if id(s) in self.id_map]

    def build_cfg(self, node):
        # create edge CFG by 3 rules
        node_type = type(node).__name__

        # Rule 1
        for field in ("body", "orelse", "finalbody"):
            stmts = self._emitted_stmts(node, field)
            for a, b in zip(stmts, stmts[1:]):
                self._emit_cfg_edge(self.id_map[id(a)], self.id_map[id(b)])

        # Rule 2
        if node_type == "If" and id(node) in self.id_map:
            for field in ("body", "orelse"):
                stmts = self._emitted_stmts(node, field)
                if stmts:
                    self._emit_cfg_edge(self.id_map[id(node)], self.id_map[id(stmts[0])])

        # Rule 3
        if node_type in ("For", "AsyncFor", "While") and id(node) in self.id_map:
            body = self._emitted_stmts(node, "body")
            if body:
                self._emit_cfg_edge(self.id_map[id(node)], self.id_map[id(body[0])])
                self._emit_cfg_edge(self.id_map[id(body[-1])], self.id_map[id(node)])

        for child in ast.iter_child_nodes(node):   # đệ quy toàn cây
            self.build_cfg(child)

    # DFG (def -> use)
    def _emit_dfg_edge(self, source_id: str, target_id: str, var: str) -> None:
        self.edges.append({
            "schema_version": "v1",
            "event_timestamp": _now_iso(),
            "edge_id": make_edge_id("DFG", source_id, target_id, var),
            "edge_type": "DFG",
            "source_node_id": source_id,
            "target_node_id": target_id,
            "dfg_variable": var,
            "file_path": self.file_path,
            "repo_commit": self.repo_commit,
        })

    @staticmethod
    def _load_names(expr):
        if expr is None:
            return []
        return [n.id for n in ast.walk(expr)
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)]

    @staticmethod
    def _store_names(targets):
        if targets is None:
            return []
        items = targets if isinstance(targets, list) else [targets]
        out = []
        for e in items:
            out += [n.id for n in ast.walk(e)
                    if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)]
        return out

    def _used_vars(self, stmt):
        t = type(stmt).__name__
        if t == "Assign":            return self._load_names(stmt.value)
        if t == "AnnAssign":         return self._load_names(stmt.value)
        if t == "AugAssign":         # x += 1
            extra = [stmt.target.id] if isinstance(stmt.target, ast.Name) else []
            return self._load_names(stmt.value) + extra
        if t == "Return":            return self._load_names(stmt.value)
        if t in ("If", "While"):     return self._load_names(stmt.test)
        if t in ("For", "AsyncFor"): return self._load_names(stmt.iter)
        return []

    def _defined_vars(self, stmt):
        t = type(stmt).__name__
        if t == "Assign":                       return self._store_names(stmt.targets)
        if t in ("AnnAssign", "AugAssign"):
            return [stmt.target.id] if isinstance(stmt.target, ast.Name) else []
        if t in ("For", "AsyncFor"):            return self._store_names(stmt.target)
        return []

    def _iter_scope_statements(self, scope_node):
        def walk_block(stmts):
            for s in stmts or []:
                yield s
                if type(s).__name__ in SCOPE_TYPES:
                    continue
                for field in ("body", "orelse", "finalbody"):
                    yield from walk_block(getattr(s, field, []))
        yield from walk_block(getattr(scope_node, "body", []))

    def _dfg_scope(self, scope_node, is_module: bool):
        last_def = {}
        if not is_module:
            scope_id = self.id_map.get(id(scope_node))
            a = scope_node.args
            for arg in list(a.posonlyargs) + list(a.args) + list(a.kwonlyargs):
                last_def[arg.arg] = scope_id
            if a.vararg: last_def[a.vararg.arg] = scope_id
            if a.kwarg:  last_def[a.kwarg.arg] = scope_id

        for stmt in self._iter_scope_statements(scope_node):
            sid = self.id_map.get(id(stmt))
            if sid is None:
                continue
            for var in self._used_vars(stmt):               # use before
                src = last_def.get(var)
                if src is not None:
                    self._emit_dfg_edge(src, sid, var)
            for var in self._defined_vars(stmt):            # def after
                last_def[var] = sid

    def build_dfg(self, tree):
        self._dfg_scope(tree, is_module=True)               # scope module
        for node in ast.walk(tree):
            if type(node).__name__ in ("FunctionDef", "AsyncFunctionDef"):
                self._dfg_scope(node, is_module=False)

    # CALL (call site -> function def)
    def _emit_call_edge(self, source_id: str, target_id: str) -> None:
        self.edges.append({
            "schema_version": "v1",
            "event_timestamp": _now_iso(),
            "edge_id": make_edge_id("CALL", source_id, target_id),
            "edge_type": "CALL",
            "source_node_id": source_id,
            "target_node_id": target_id,
            "dfg_variable": None,
            "file_path": self.file_path,
            "repo_commit": self.repo_commit,
        })

    def build_call(self, tree):
        # index: function name -> FunctionDef node_id
        func_index = {}
        for node in ast.walk(tree):
            if type(node).__name__ in ("FunctionDef", "AsyncFunctionDef"):
                fid = self.id_map.get(id(node))
                if fid is not None:
                    func_index[node.name] = fid

        # each Call node -> FunctionDef same name (if defined in this file)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                callee = None
                if isinstance(node.func, ast.Name):
                    callee = node.func.id            # g(x)
                elif isinstance(node.func, ast.Attribute):
                    callee = node.func.attr          # obj.method()
                cid = self.id_map.get(id(node))
                if callee and cid is not None and callee in func_index:
                    self._emit_call_edge(cid, func_index[callee])

    def analyze(self, tree) -> None:
        # node + edge AST + CFG + DFG + CALL
        self.visit(tree)        # pass 1
        self.build_cfg(tree)    # pass 2
        self.build_dfg(tree)    # pass 3
        self.build_call(tree)   # pass 4


    def generic_visit(self, node):
        node_type = type(node).__name__

        emitted_id = None
        if node_type in INTERESTING:
            emitted_id = self._emit(node, node_type)         # create node
            if self.node_id_stack:                            # match nearest parent -> this node
                self._emit_ast_edge(self.node_id_stack[-1], emitted_id)

        pushed_scope = False
        if node_type in SCOPE_TYPES:
            self.scope_stack.append(node.name)
            pushed_scope = True

        if emitted_id:
            self.node_id_stack.append(emitted_id)             # child can find parent

        super().generic_visit(node)                           # traversal

        if emitted_id:
            self.node_id_stack.pop()
        if pushed_scope:
            self.scope_stack.pop()

def _file_hash(source: str) -> str:
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def parse_source(source: str, file_path: str, repo_commit: str = "dev", repo: str | None = None):
    # core: source text -> (nodes, edges, metadata)
    tree = ast.parse(source, filename=file_path)
    visitor = CPGVisitor(file_path, repo_commit, repo)
    visitor.analyze(tree)

    metadata = {
        "schema_version": "v1",
        "event_timestamp": _now_iso(),
        "file_path": file_path,
        "file_hash": _file_hash(source),
        "language": "python",
        "loc": len(source.splitlines()),
        "num_nodes": len(visitor.nodes),
        "num_edges": len(visitor.edges),
        "repo_commit": repo_commit,
    }
    if repo:
        metadata["repo"] = repo
    return visitor.nodes, visitor.edges, metadata


def parse_file(file_path: str, repo_commit: str = "dev", repo: str | None = None):
    with open(file_path, "r", encoding="utf-8") as f:
        source = f.read()
    return parse_source(source, file_path, repo_commit, repo)


# Đoạn code mẫu để chạy thử nhanh khi không truyền file.
SAMPLE = '''
class Model:
    def forward(self, x):
        if x > 0:
            y = x + 1
        else:
            y = g(x)
        return y

def g(v):
    return v * 2
'''


if __name__ == "__main__":
    meta = None
    if len(sys.argv) > 1:
        nodes, edges, meta = parse_file(sys.argv[1])
    else:
        tree = ast.parse(SAMPLE, filename="<sample>")
        v = CPGVisitor("<sample>", "dev")
        v.analyze(tree)
        nodes, edges = v.nodes, v.edges

    out = {"nodes": nodes, "edges": edges}
    if meta:
        out["metadata"] = meta
    print(json.dumps(out, indent=2, ensure_ascii=False))
    by_type = Counter(e["edge_type"] for e in edges)
    print(f"\n--- {len(nodes)} node, {len(edges)} cạnh {dict(by_type)} ---")




