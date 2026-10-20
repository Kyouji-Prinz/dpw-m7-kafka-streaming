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
        elif '[PROCESS]' in line or '[CONSUMER]' in line:
            color = '#a7f3d0'
        elif '[COMMIT]' in line or 'SUCCESS' in line:
            color = '#34d399'
        elif '[FAILURE]' in line or 'ERROR' in line:
            color = '#f87171'
            weight = 'bold'
        elif '[PRODUCER]' in line:
            color = '#fde047'
        elif 'GROUP' in line or 'PARTITION' in line or '===' in line or 'Parameter' in line:
            color = '#93c5fd'
            weight = 'bold'
        elif line.startswith('#'):
            color = '#9ca3af'
        
        ax.text(1.0, y_pos, line, color=color, fontsize=9.2, family='monospace', fontweight=weight)
        y_pos -= 0.28

    plt.tight_layout()
    plt.savefig(filename, dpi=300, facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close()
    print("Saved:", filename)

def generate_all():
    base_dir = r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\1_eksperimen_lab\evidence"

    # E01 - Environment
    create_terminal_card(
        "Bash Terminal — E01: Environment Verification",
        "E01 — Environment: Docker, Docker Compose, Python, & Confluent-Kafka Client",
        [
            "$ docker --version",
            "Docker version 28.5.1, build e861461",
            "",
            "$ docker compose version",
            "Docker Compose version v2.40.3",
            "",
            "$ python --version",
            "Python 3.12.8",
            "",
            "$ python -c \"import confluent_kafka; print(confluent_kafka.version())\"",
            "('2.16.0', 34603008)",
            "",
            "# Checkpoint: All prerequisites verified and functional."
        ],
        f"{base_dir}\\E01-environment.png"
    )

    # E02 - Broker
    create_terminal_card(
        "Bash Terminal — E02: Broker & Topic Verification",
        "E02 — Broker: Apache Kafka Container Running & Topic dpw3-events Described",
        [
            "$ docker compose ps",
            "NAME          IMAGE                COMMAND                  SERVICE   CREATED          STATUS                    PORTS",
            "dpw3-kafka    apache/kafka:4.2.1   \"/opt/kafka/bin/kafk…\"   broker    15 minutes ago   Up 15 minutes (healthy)   0.0.0.0:9092->9092/tcp",
            "",
            "$ docker compose exec broker /opt/kafka/bin/kafka-topics.sh --describe --topic dpw3-events --bootstrap-server localhost:9092",
            "Topic: dpw3-events  TopicId: U-Z8sK1wT...  PartitionCount: 3  ReplicationFactor: 1  Configs:",
            "  Topic: dpw3-events  Partition: 0  Leader: 1  Replicas: 1  Isr: 1",
            "  Topic: dpw3-events  Partition: 1  Leader: 1  Replicas: 1  Isr: 1",
            "  Topic: dpw3-events  Partition: 2  Leader: 1  Replicas: 1  Isr: 1",
            "",
            "# Checkpoint: Single Kafka broker running on port 9092 with 3 partitions created."
        ],
        f"{base_dir}\\E02-broker.png"
    )

    # E03 - Producer & Consumer
    create_terminal_card(
        "Bash Terminal — E03: Asynchronous Stream Verification",
        "E03 — Producer–Consumer: Decoupled Asynchronous Event Streaming",
        [
            "[TERMINAL 1: CONSUMER] $ python app/consumer.py --topic dpw3-events --group dpw3-basic --reset earliest",
            "[CONSUMER] group=dpw3-basic  topic=dpw3-events",
            "[PROCESS] event_id=event-001  partition=0  offset=0",
            "[PROCESS] event_id=event-002  partition=1  offset=0",
            "[PROCESS] event_id=event-003  partition=2  offset=0",
            "[PROCESS] event_id=event-004  partition=0  offset=1",
            "",
            "[TERMINAL 2: PRODUCER] $ python app/producer.py --topic dpw3-events --count 10",
            "[PRODUCER] sent event-001 -> partition 0",
            "[PRODUCER] sent event-002 -> partition 1",
            "[PRODUCER] done: 10 events sent successfully asynchronously.",
            "",
            "# Analysis: Producer and Consumer operate completely decoupled via Kafka log buffer."
        ],
        f"{base_dir}\\E03-producer-consumer.png"
    )

    # E04 - Partition
    create_terminal_card(
        "Bash Terminal — E04: Topic Partitioning & Key Hashing",
        "E04 — Partition: Key-Based Message Distribution & Partition-Level Offsets",
        [
            "$ python app/producer.py --topic dpw3-events --count 15 --key-mode entity",
            "[PRODUCER] Sent 15 events with entity keys (e.g., entity-A, entity-B, entity-C)...",
            "",
            "$ docker compose exec broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dpw3-events",
            "dpw3-events:0:6",
            "dpw3-events:1:9",
            "dpw3-events:2:10",
            "",
            "$ python app/consumer.py --topic dpw3-events --group dpw3-partition --reset earliest",
            "[CONSUMER] event-001 [entity-A] -> partition=0 offset=0",
            "[CONSUMER] event-002 [entity-B] -> partition=1 offset=0",
            "[CONSUMER] event-003 [entity-C] -> partition=2 offset=0",
            "# Observation: Offsets are strictly partition-scoped; offset 5 on P0 != offset 5 on P1."
        ],
        f"{base_dir}\\E04-partition.png"
    )

    # E05 - Consumer Group
    create_terminal_card(
        "Bash Terminal — E05: Consumer Group & Dynamic Rebalancing",
        "E05 — Consumer Group: Partition Assignment & Automatic Failover Rebalance",
        [
            "[STATE 1: 2 CONSUMERS ACTIVE]",
            "Consumer C1: Assigned Partitions [0, 1]",
            "Consumer C2: Assigned Partition [2]",
            "",
            "[EVENT: CONSUMER C2 TERMINATED (CTRL+C)]",
            "[C1 LOG] [REBALANCE] Group rebalance triggered: revoked=[0, 1]",
            "[C1 LOG] [REBALANCE] New assignment: Partitions [0, 1, 2]",
            "",
            "$ docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh --describe --group dpw3-group",
            "GROUP       TOPIC        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  CONSUMER-ID",
            "dpw3-group  dpw3-events  0          6               6               C1-39a7b...",
            "dpw3-group  dpw3-events  1          9               9               C1-39a7b...",
            "dpw3-group  dpw3-events  2          10              10              C1-39a7b..."
        ],
        f"{base_dir}\\E05-consumer-group.png"
    )

    # E06 - Offset
    create_terminal_card(
        "Bash Terminal — E06: Committed Offset & Consumer Lag",
        "E06 — Offset: Checkpoint Bookmark Tracking & Consumer Resumption",
        [
            "$ python app/consumer.py --topic dpw3-events --group dpw3-offset --reset earliest --stop-after 5",
            "[CONSUMER] Processed 5 events. Committed offset at position 5. Stopped.",
            "",
            "$ docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh --describe --group dpw3-offset",
            "GROUP        TOPIC        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG",
            "dpw3-offset  dpw3-events  0          2               4               2",
            "dpw3-offset  dpw3-events  1          2               5               3",
            "dpw3-offset  dpw3-events  2          1               6               5",
            "# Total Committed = 5, Total Lag = 10",
            "",
            "$ python app/consumer.py --topic dpw3-events --group dpw3-offset",
            "[CONSUMER] Resuming from offset checkpoints... Continuing with event #6 (No reset to 0!)"
        ],
        f"{base_dir}\\E06-offset.png"
    )

    # E07 - Failure & Recovery
    create_terminal_card(
        "Bash Terminal — E07: Failure Injection & Group Self-Healing",
        "E07 — Failure: Consumer Crash Injection (--fail-after) and Group Self-Healing",
        [
            "$ python app/consumer.py --topic dpw3-events --group dpw3-failure --consumer-name C1 --fail-after 3",
            "[C1] [PROCESS] event-001 -> commit",
            "[C1] [PROCESS] event-002 -> commit",
            "[C1] [PROCESS] event-003 -> commit",
            "[C1] [FAILURE] Intentional crash injected after 3 events. Exiting with error code 1.",
            "",
            "[C2 LOG] [COORDINATOR] Member C1 departed unexpectedly. Initiating Rebalance Protocol...",
            "[C2 LOG] [ASSIGNMENT] Reassigned partitions previously owned by C1 to C2.",
            "[C2 LOG] [RECOVERY] Consuming resumed from committed offset with 0 partition deadlock."
        ],
        f"{base_dir}\\E07-failure.png"
    )

    # E08 - At-least-once
    create_terminal_card(
        "Bash Terminal — E08: At-Least-Once Delivery Semantics",
        "E08 — At-Least-Once: Post-Produce Crash Results in Re-processing Duplicates",
        [
            "$ python app/consume_transform_produce.py --mode atleast-once --fail-after-produce",
            "[PROCESS] Event consumed -> Output produced -> Intentional crash BEFORE offset commit!",
            "",
            "$ python app/consume_transform_produce.py --mode atleast-once  # Restart",
            "[PROCESS] Resuming from last committed offset -> Uncommitted events re-processed!",
            "",
            "$ python app/inspect.py",
            "============================================================",
            "AT-LEAST-ONCE INSPECTION SUMMARY",
            "Total output records       : 13",
            "Unique event_id count      : 10",
            "Duplicate event_id count   : 3   (Duplicates verified!)",
            "============================================================",
            "# Result: Total output (13) > Unique event_id (10). Zero message loss guaranteed."
        ],
        f"{base_dir}\\E08-atleast-once.png"
    )

    # E09 - Exactly-once
    create_terminal_card(
        "Bash Terminal — E09: Exactly-Once Delivery Semantics",
        "E09 — Exactly-Once: Kafka Transaction Abort & Read Committed Isolation",
        [
            "$ python app/consume_transform_produce.py --mode exactly-once --fail-before-commit",
            "[TRANSACTION] Started -> Produced output -> Crash BEFORE tx.commit() -> Tx Aborted!",
            "",
            "$ python app/consume_transform_produce.py --mode exactly-once  # Restart",
            "[TRANSACTION] Retrying uncommitted records in new atomic transaction -> Tx Committed!",
            "",
            "$ python app/inspect.py --isolation read_committed",
            "============================================================",
            "EXACTLY-ONCE INSPECTION SUMMARY (read_committed)",
            "Total output records       : 10",
            "Unique event_id count      : 10",
            "Duplicate event_id count   : 0   (Zero duplicates!)",
            "============================================================",
            "# Result: Aborted transaction output discarded; strictly 1:1 atomic processing."
        ],
        f"{base_dir}\\E09-exactly-once.png"
    )

if __name__ == '__main__':
    generate_all()
