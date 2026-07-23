// Unique Constraint for CPG Nodes based on stable node_id
CREATE CONSTRAINT unique_cpg_node_id IF NOT EXISTS
FOR (n:CPGNode) REQUIRE n.node_id IS UNIQUE;

// Index for CPG Edge lookup by edge_id (Optimizes MERGE on relationships)
CREATE INDEX cpg_edge_id_idx IF NOT EXISTS
FOR ()-[r:CPG_EDGE]-() ON (r.edge_id);
