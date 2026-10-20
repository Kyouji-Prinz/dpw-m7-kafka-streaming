import matplotlib.pyplot as plt
import matplotlib.patches as patches

def draw_architecture():
    fig, ax = plt.subplots(figsize=(14, 9), dpi=300)
    fig.patch.set_facecolor('#0f172a')
    ax.set_facecolor('#0f172a')
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 9)
    ax.axis('off')

    # Title
    ax.text(7, 8.5, "M7 ARCHITECTURE: PAYMENT EVENT STREAMING PIPELINE", 
            ha='center', va='center', color='#38bdf8', fontsize=16, fontweight='bold')
    ax.text(7, 8.1, "Apache Kafka Topic Partitions, Strict FIFO per Order_ID, & At-Least-Once Fault Recovery", 
            ha='center', va='center', color='#94a3b8', fontsize=11)

    # Box 1: Producer
    rect_prod = patches.FancyBboxPatch((0.5, 4.2), 2.5, 3.2, boxstyle="round,pad=0.2", 
                                       facecolor='#1e293b', edgecolor='#38bdf8', linewidth=2)
    ax.add_patch(rect_prod)
    ax.text(1.75, 7.1, "EVENT PRODUCER", ha='center', va='center', color='#38bdf8', fontsize=12, fontweight='bold')
    ax.text(1.75, 6.7, "app/producer.py", ha='center', va='center', color='#64748b', fontsize=9, family='monospace')
    ax.text(1.75, 5.8, "• Reads payment-events.json\n• Key: event['order_id']\n• Murmur2 Partitioner\n• 8 Events sent (9 JSON records)\n• Async Flush & Poll", 
            ha='center', va='center', color='#f1f5f9', fontsize=9)
    ax.text(1.75, 4.5, "[ORD-01, ORD-02, ORD-03]", ha='center', va='center', color='#a855f7', fontsize=9, fontweight='bold')

    # Box 2: Kafka Broker / Topic
    rect_topic = patches.FancyBboxPatch((4.2, 1.8), 5.6, 5.6, boxstyle="round,pad=0.2", 
                                        facecolor='#1e293b', edgecolor='#f59e0b', linewidth=2)
    ax.add_patch(rect_topic)
    ax.text(7.0, 7.1, "KAFKA BROKER: TOPIC dpw3-payment-events", ha='center', va='center', color='#fbbf24', fontsize=12, fontweight='bold')
    ax.text(7.0, 6.7, "localhost:9092 | Partitions: 3 | Replication: 1", ha='center', va='center', color='#64748b', fontsize=9, family='monospace')

    # Partitions inside Topic
    # Partition 0
    p0 = patches.FancyBboxPatch((4.5, 5.1), 5.0, 1.2, boxstyle="round,pad=0.1", 
                                facecolor='#0f172a', edgecolor='#3b82f6', linewidth=1.5)
    ax.add_patch(p0)
    ax.text(4.7, 5.9, "PARTITION 0  [Key: ORD-01]", color='#60a5fa', fontsize=10, fontweight='bold')
    ax.text(4.7, 5.4, "Offsets 3,4,5: PAY-001 (CREATED) -> PAY-003 (PAID) -> PAY-006 (SHIPPED)", color='#cbd5e1', fontsize=8.5, family='monospace')

    # Partition 1
    p1 = patches.FancyBboxPatch((4.5, 3.6), 5.0, 1.2, boxstyle="round,pad=0.1", 
                                facecolor='#0f172a', edgecolor='#ec4899', linewidth=1.5)
    ax.add_patch(p1)
    ax.text(4.7, 4.4, "PARTITION 1  [Key: ORD-03]", color='#f472b6', fontsize=10, fontweight='bold')
    ax.text(4.7, 3.9, "Offsets 2,3: PAY-004 (CREATED) -> PAY-007 (PAID)", color='#cbd5e1', fontsize=8.5, family='monospace')

    # Partition 2
    p2 = patches.FancyBboxPatch((4.5, 2.1), 5.0, 1.2, boxstyle="round,pad=0.1", 
                                facecolor='#0f172a', edgecolor='#10b981', linewidth=1.5)
    ax.add_patch(p2)
    ax.text(4.7, 2.9, "PARTITION 2  [Key: ORD-02]", color='#34d399', fontsize=10, fontweight='bold')
    ax.text(4.7, 2.4, "Offsets 3,4,5: PAY-002 (CREATED) -> PAY-005 (PAID) -> PAY-008 (SHIPPED)", color='#cbd5e1', fontsize=8.5, family='monospace')

    # Box 3: Consumer Group & Worker
    rect_cons = patches.FancyBboxPatch((10.8, 3.2), 2.7, 4.2, boxstyle="round,pad=0.2", 
                                       facecolor='#1e293b', edgecolor='#10b981', linewidth=2)
    ax.add_patch(rect_cons)
    ax.text(12.15, 7.1, "CONSUMER GROUP", ha='center', va='center', color='#10b981', fontsize=12, fontweight='bold')
    ax.text(12.15, 6.7, "group.id: dpw3-case", ha='center', va='center', color='#64748b', fontsize=9, family='monospace')
    ax.text(12.15, 5.7, "• Consumer Instance (C1)\n• enable.auto.commit = False\n• Manual Commit per msg\n• At-least-once Semantics\n• Strict FIFO Verification\n• State Engine Transition", 
            ha='center', va='center', color='#f1f5f9', fontsize=9)
    ax.text(12.15, 3.8, "Lag = 0 (All Partitions)", ha='center', va='center', color='#22c55e', fontsize=9.5, fontweight='bold')

    # Box 4: Bottom Banner (Failure Injection & Semantics)
    rect_fail = patches.FancyBboxPatch((0.5, 0.4), 13.0, 1.2, boxstyle="round,pad=0.2", 
                                       facecolor='#1e293b', edgecolor='#ef4444', linewidth=1.5)
    ax.add_patch(rect_fail)
    ax.text(0.8, 1.2, "FAILURE & RECOVERY EVIDENCE CHAIN:", color='#f87171', fontsize=10, fontweight='bold')
    ax.text(0.8, 0.7, "1. Crash Simulation (FAIL_AFTER=2): C1 halted on PAY-002 before commit at P2:Offset 3.\n"
                      "2. Recovery: Consumer resumes from committed offset 3, safely re-processes PAY-002, commits, and finishes stream with 0 message loss.\n"
                      "3. Ordering Guarantee: Key=order_id guarantees intra-order FIFO across all states (CREATED -> PAID -> SHIPPED).", 
            color='#e2e8f0', fontsize=8.5)

    # Arrows
    # Producer -> Partitions
    ax.annotate("", xy=(4.4, 5.7), xytext=(3.1, 6.2),
                arrowprops=dict(arrowstyle="->", color="#60a5fa", lw=2, mutation_scale=15))
    ax.annotate("", xy=(4.4, 4.2), xytext=(3.1, 5.8),
                arrowprops=dict(arrowstyle="->", color="#f472b6", lw=2, mutation_scale=15))
    ax.annotate("", xy=(4.4, 2.7), xytext=(3.1, 5.4),
                arrowprops=dict(arrowstyle="->", color="#34d399", lw=2, mutation_scale=15))

    # Partitions -> Consumer
    ax.annotate("", xy=(10.7, 5.8), xytext=(9.6, 5.7),
                arrowprops=dict(arrowstyle="->", color="#60a5fa", lw=2, mutation_scale=15))
    ax.annotate("", xy=(10.7, 5.3), xytext=(9.6, 4.2),
                arrowprops=dict(arrowstyle="->", color="#f472b6", lw=2, mutation_scale=15))
    ax.annotate("", xy=(10.7, 4.8), xytext=(9.6, 2.7),
                arrowprops=dict(arrowstyle="->", color="#34d399", lw=2, mutation_scale=15))

    plt.tight_layout()
    output_path = r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\2_tugas_mandiri\submission\architecture.png"
    plt.savefig(output_path, dpi=300, facecolor=fig.get_facecolor(), edgecolor='none')
    print("architecture.png successfully saved to:", output_path)

if __name__ == "__main__":
    draw_architecture()
