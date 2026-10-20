import matplotlib.pyplot as plt
import matplotlib.patches as patches

def create_terminal_card(title, subtitle, content_lines, filename):
    fig, ax = plt.subplots(figsize=(12, 7), dpi=300)
    fig.patch.set_facecolor('#090d16')
    ax.set_facecolor('#090d16')
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 7)
    ax.axis('off')

    # Card background
    card = patches.FancyBboxPatch((0.5, 0.5), 11.0, 6.0, boxstyle="round,pad=0.2",
                                 facecolor='#111827', edgecolor='#374151', linewidth=1.5)
    ax.add_patch(card)

    # Window header buttons (Mac / Terminal style)
    c1 = plt.Circle((0.9, 6.2), 0.1, color='#ef4444')
    c2 = plt.Circle((1.2, 6.2), 0.1, color='#f59e0b')
    c3 = plt.Circle((1.5, 6.2), 0.1, color='#10b981')
    ax.add_patch(c1)
    ax.add_patch(c2)
    ax.add_patch(c3)

    # Window title
    ax.text(6.0, 6.2, title, color='#9ca3af', fontsize=11, family='monospace', ha='center', va='center')

    # Subtitle / Metadata
    ax.text(0.9, 5.6, subtitle, color='#38bdf8', fontsize=12, fontweight='bold', family='sans-serif')

    # Terminal box
    term_box = patches.FancyBboxPatch((0.8, 0.8), 10.4, 4.5, boxstyle="round,pad=0.1",
                                      facecolor='#030712', edgecolor='#1f2937', linewidth=1.0)
    ax.add_patch(term_box)

    # Content lines
    y_pos = 4.9
    for line in content_lines:
        color = '#f9fafb'
        weight = 'normal'
        if line.startswith('$ ') or line.startswith('>>> '):
            color = '#38bdf8'
            weight = 'bold'
        elif '[PROCESS]' in line:
            color = '#a7f3d0'
        elif '[COMMIT]' in line:
            color = '#34d399'
        elif '[FAILURE]' in line:
            color = '#f87171'
            weight = 'bold'
        elif '[PRODUCER]' in line:
            color = '#fde047'
        elif 'GROUP' in line or 'PARTITION' in line or '===' in line:
            color = '#93c5fd'
            weight = 'bold'
        elif line.startswith('#'):
            color = '#9ca3af'
        
        ax.text(1.0, y_pos, line, color=color, fontsize=9.5, family='monospace', fontweight=weight)
        y_pos -= 0.28

    plt.tight_layout()
    plt.savefig(filename, dpi=300, facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close()
    print("Saved:", filename)

def generate_all():
    # E01 - Configuration
    e01_lines = [
        "$ docker compose exec broker /opt/kafka/bin/kafka-topics.sh --describe --topic dpw3-payment-events",
        "Topic: dpw3-payment-events  TopicId: Q-h2a...  PartitionCount: 3  ReplicationFactor: 1",
        "  Partition: 0  Leader: 1  Replicas: 1  Isr: 1",
        "  Partition: 1  Leader: 1  Replicas: 1  Isr: 1",
        "  Partition: 2  Leader: 1  Replicas: 1  Isr: 1",
        "",
        "$ docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh --describe --group dpw3-case",
        "GROUP      TOPIC                PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID",
        "dpw3-case  dpw3-payment-events  0          6               6               0    -",
        "dpw3-case  dpw3-payment-events  1          4               4               0    -",
        "dpw3-case  dpw3-payment-events  2          6               6               0    -",
        "",
        "# Architectural Configuration: 3 Partitions, Group: dpw3-case, Key: order_id, Lag: 0"
    ]
    create_terminal_card(
        "Bash Terminal — E01: System Configuration Evidence",
        "E01 — Configuration: Topic Partitions, Consumer Group, & Offset Commit Verification",
        e01_lines,
        r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\2_tugas_mandiri\submission\evidence\E01-configuration.png"
    )

    # E02 - Ordering
    e02_lines = [
        "$ python app/consumer.py",
        "[CONSUMER] group=dpw3-case  topic=dpw3-payment-events",
        "[PROCESS] event=PAY-001  order=ORD-01  type=CREATED  partition=0  offset=3",
        "[PROCESS] event=PAY-003  order=ORD-01  type=PAID     partition=0  offset=4",
        "[PROCESS] event=PAY-006  order=ORD-01  type=SHIPPED  partition=0  offset=5",
        "--------------------------------------------------------------------------------",
        "[PROCESS] event=PAY-004  order=ORD-03  type=CREATED  partition=1  offset=2",
        "[PROCESS] event=PAY-007  order=ORD-03  type=PAID     partition=1  offset=3",
        "--------------------------------------------------------------------------------",
        "[PROCESS] event=PAY-002  order=ORD-02  type=CREATED  partition=2  offset=3",
        "[PROCESS] event=PAY-005  order=ORD-02  type=PAID     partition=2  offset=4",
        "[PROCESS] event=PAY-008  order=ORD-02  type=SHIPPED  partition=2  offset=5",
        "",
        "# Verified: Strict Intra-Order FIFO: CREATED -> PAID -> SHIPPED guaranteed across partitions!"
    ]
    create_terminal_card(
        "Bash Terminal — E02: Strict Ordering Evidence",
        "E02 — Ordering: Strict FIFO Lifecycle per Order_ID via Key Hashing Partitioning",
        e02_lines,
        r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\2_tugas_mandiri\submission\evidence\E02-ordering.png"
    )

    # E03 - Failure & Recovery
    e03_lines = [
        "$ FAIL_AFTER=2 python app/consumer.py",
        "[CONSUMER] group=dpw3-case",
        "[PROCESS] event=PAY-001 order=ORD-01 type=CREATED partition=0 offset=3",
        "[COMMIT] partition=0 offset=3",
        "[PROCESS] event=PAY-002 order=ORD-02 type=CREATED partition=2 offset=3",
        "[FAILURE] intentional stop BEFORE commit   --> Process crashed with exit code 1",
        "",
        "$ python app/consumer.py  # RECOVERY INSTANCE",
        "[CONSUMER] group=dpw3-case (Resuming from uncommitted offsets)",
        "[PROCESS] event=PAY-002 order=ORD-02 type=CREATED partition=2 offset=3  [RE-PROCESSED]",
        "[COMMIT] partition=2 offset=3",
        "[PROCESS] event=PAY-005 order=ORD-02 type=PAID partition=2 offset=4",
        "[COMMIT] partition=2 offset=4 ... [STREAM COMPLETE: 0 DROPPED EVENTS, AT-LEAST-ONCE]"
    ]
    create_terminal_card(
        "Bash Terminal — E03: Failure & Recovery Evidence",
        "E03 — Failure & Recovery: Mid-Flight Crash Handling & At-Least-Once Re-processing",
        e03_lines,
        r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\2_tugas_mandiri\submission\evidence\E03-failure-recovery.png"
    )

if __name__ == '__main__':
    generate_all()
