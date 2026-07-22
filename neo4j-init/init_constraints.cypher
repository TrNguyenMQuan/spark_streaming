// Unique Constraint for CPG Nodes based on stable node_id
CREATE CONSTRAINT unique_cpg_node_id IF NOT EXISTS
FOR (n:CPGNode) REQUIRE n.node_id IS UNIQUE;
