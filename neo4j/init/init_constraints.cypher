// Unique Constraint for CPG Nodes based on stable node_id
CREATE CONSTRAINT unique_cpg_node_id IF NOT EXISTS
FOR (n:CPGNode) REQUIRE n.node_id IS UNIQUE;

// Indexes for CPG Edge lookup by edge_id (Optimizes MERGE on relationships)
CREATE INDEX ast_edge_id_idx IF NOT EXISTS FOR ()-[r:AST]-() ON (r.edge_id);
CREATE INDEX cfg_edge_id_idx IF NOT EXISTS FOR ()-[r:CFG]-() ON (r.edge_id);
CREATE INDEX dfg_edge_id_idx IF NOT EXISTS FOR ()-[r:DFG]-() ON (r.edge_id);
CREATE INDEX call_edge_id_idx IF NOT EXISTS FOR ()-[r:CALL]-() ON (r.edge_id);
