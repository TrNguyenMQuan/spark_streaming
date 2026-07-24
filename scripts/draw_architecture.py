import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pathlib import Path

def draw_architecture_diagram():
    fig, ax = plt.subplots(figsize=(14, 10), dpi=300)
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 10)
    ax.axis("off")

    # Title
    ax.text(7, 9.6, "Lab 04: Incremental CPG Streaming Pipeline Architecture", 
            ha="center", va="center", fontsize=16, fontweight="bold", color="#1E293B")

    def draw_box(x, y, w, h, title, subtitle="", bg_color="#E2E8F0", border_color="#64748B", text_color="#0F172A"):
        rect = patches.FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.2", 
                                      ec=border_color, fc=bg_color, lw=1.5)
        ax.add_patch(rect)
        if subtitle:
            ax.text(x + w/2, y + h*0.65, title, ha="center", va="center", 
                    fontsize=10, fontweight="bold", color=text_color)
            ax.text(x + w/2, y + h*0.3, subtitle, ha="center", va="center", 
                    fontsize=8, color="#475569", style="italic")
        else:
            ax.text(x + w/2, y + h/2, title, ha="center", va="center", 
                    fontsize=9, fontweight="bold", color=text_color)

    def draw_arrow(x1, y1, x2, y2, label="", color="#334155"):
        ax.annotate(
            "", xy=(x2, y2), xytext=(x1, y1),
            arrowprops=dict(arrowstyle="-|>", color=color, lw=1.5, mutation_scale=12)
        )
        if label:
            mx, my = (x1 + x2)/2, (y1 + y2)/2
            ax.text(mx, my + 0.15, label, ha="center", va="bottom", fontsize=8, color="#334155", fontweight="bold")

    # --- 1. Source Repository ---
    draw_box(0.5, 7.2, 2.5, 1.4, "Target Repository", ".py Source Files", bg_color="#F1F5F9", border_color="#94A3B8")

    # --- 2. Parser Service ---
    draw_box(0.5, 4.2, 2.5, 2.2, "Parser Service\n(Task 1 & 2)", "discover_files.py\ncpg_visitor.py (AST/CFG/DFG)\nstable_id.py (SHA-256)", bg_color="#E0F2FE", border_color="#0284C7", text_color="#0369A1")

    # --- 3. Kafka Topics ---
    draw_box(4.2, 7.8, 2.6, 0.8, "cpg.nodes.v1", "AST/CPG Node Events", bg_color="#FFEDD5", border_color="#F97316")
    draw_box(4.2, 6.4, 2.6, 0.8, "cpg.edges.v1", "CFG/DFG/Call Edges", bg_color="#FFEDD5", border_color="#F97316")
    draw_box(4.2, 5.0, 2.6, 0.8, "cpg.metadata.v1", "Source File Metadata", bg_color="#FEF3C7", border_color="#D97706")
    draw_box(4.2, 3.6, 2.6, 0.8, "cpg.errors.v1", "Parser Errors", bg_color="#FEE2E2", border_color="#EF4444")

    # --- 4. Ingestion Engines ---
    draw_box(8.0, 6.6, 2.5, 1.8, "Neo4j Kafka Connect\nSink Connector\n(Task 4)", "Direct Ingestion\n(No Spark Layer)", bg_color="#DCFCE7", border_color="#16A34A", text_color="#15803D")
    draw_box(8.0, 4.4, 2.5, 1.8, "Apache Spark\nStructured Streaming\n(Task 5)", "metadata_to_mongodb.py\n+ Checkpointing", bg_color="#F3E8FF", border_color="#9333EA", text_color="#7E22CE")

    # --- 5. Target Databases ---
    draw_box(11.4, 6.6, 2.1, 1.8, "Neo4j Graph DB", "CPG Nodes & Edges\n(Cypher MERGE)", bg_color="#BBF7D0", border_color="#15803D")
    draw_box(11.4, 4.4, 2.1, 1.8, "MongoDB DB", "source_metadata\nCollection", bg_color="#E9D5FF", border_color="#7E22CE")

    # --- 6. Verification Suite ---
    draw_box(5.5, 0.8, 5.0, 1.8, "Idempotent Replay Verification Test Suite (Task 6)", "scripts/tests/ (TC1: Add Function, TC2: Add Class, TC3: Line Shift,\nTC4: Exact Replay, TC5: Call Graph, Audit Accuracy)", bg_color="#FEF9C3", border_color="#CA8A04", text_color="#854D0E")

    # --- Connections ---
    draw_arrow(1.75, 7.2, 1.75, 6.4, "Cloning")
    
    # Producer to Kafka
    draw_arrow(3.0, 5.8, 4.2, 8.2)
    draw_arrow(3.0, 5.5, 4.2, 6.8)
    draw_arrow(3.0, 5.0, 4.2, 5.4)
    draw_arrow(3.0, 4.5, 4.2, 4.0)

    # Kafka to Ingestion
    draw_arrow(6.8, 8.2, 8.0, 7.8)
    draw_arrow(6.8, 6.8, 8.0, 7.2)
    draw_arrow(6.8, 5.4, 8.0, 5.3)

    # Ingestion to Databases
    draw_arrow(10.5, 7.5, 11.4, 7.5, "Cypher Sink")
    draw_arrow(10.5, 5.3, 11.4, 5.3, "Spark Connector")

    # Verification to Databases
    draw_arrow(10.5, 2.4, 12.0, 6.6, "Query Cypher")
    draw_arrow(10.5, 2.6, 12.0, 4.4, "Query Metadata")

    plt.tight_layout()
    output_dir = Path("docs")
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / "architecture_diagram.png"
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Successfully generated architecture diagram image at: {output_path.resolve()}")

if __name__ == "__main__":
    draw_architecture_diagram()
