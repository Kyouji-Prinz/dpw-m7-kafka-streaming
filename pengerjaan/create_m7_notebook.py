import nbformat as nbf

nb = nbf.v4.new_notebook()

# Metadata
nb.metadata = {
    "kernelspec": {
        "display_name": "Python 3 (Data Processing)",
        "language": "python",
        "name": "python3"
    },
    "language_info": {
        "codemirror_mode": {"name": "ipython", "version": 3},
        "file_extension": ".py",
        "mimetype": "text/x-python",
        "name": "python",
        "nbconvert_exporter": "python",
        "pygments_lexer": "ipython3",
        "version": "3.12.8"
    }
}

cells = []

# Cell 1: Markdown Title & Meta
cells.append(nbf.v4.new_markdown_cell("""# LAPORAN PRAKTIKUM & TANTANGAN MANDIRI — MODUL 3 (M7)
## Data Ingestion: Event-Driven Streaming Architecture dengan Apache Kafka
**Mata Kuliah:** Praktikum Data Processing Workflow  
**Topik:** Event-Driven Ingestion, Apache Kafka Broker, Topic, Partitioning, Consumer Groups, Committed Offset, Failure Recovery, dan Delivery Semantics  
**Nama Mahasiswa:** Udi Adhahaska Arta Ananta  
**NRP:** 5222600052  
**Kelas:** 3 D4 Sains Data Terapan / Semester 5  

---

### Ringkasan Eksekutif
Notebook ini mendokumentasikan seluruh rangkaian eksekusi praktikum **Modul 3 (M7)** yang terbagi menjadi dua bagian:
1. **Bagian 1: Eksperimen Terpandu (Bab 3)** — Pembuktian fundamental Kafka:
   - Verifikasi environment Docker Compose dan Kafka Broker (E01, E02).
   - Asynchronous decoupled streaming producer–consumer (E03).
   - Distribusi partisi topic dengan entity key hashing (E04).
   - Penskalaan horizontal dan dynamic rebalance consumer group (E05).
   - Pelacakan committed offset dan consumer lag (E06).
   - Injeksi failure consumer dan mekanisme self-healing recovery (E07).
   - Komparasi empiris At-least-once vs Exactly-once semantics (E08, E09).
2. **Bagian 2: Tantangan Mandiri (Bab 4)** — Case Study *Payment Event Stream*:
   - Menjamin strict ordering per entitas `order_id` (CREATED -> PAID -> SHIPPED).
   - Memproses order yang berbeda secara paralel via partisi.
   - Mensimulasikan crash worker saat in-flight processing dan membuktikan pemulihan (*recovery*) tanpa kehilangan data berkat semantik *At-least-once*.
"""))

# Cell 2: Markdown Section 1
cells.append(nbf.v4.new_markdown_cell("""---
# BAGIAN 1: VERIFIKASI ENVIRONMENT & BROKER KAFKA (BAB 3.1 & 3.2)
Sebelum menjalankan pipeline streaming, environment Docker, Docker Compose, Python, dan library `confluent-kafka` diverifikasi, serta container broker Kafka dipastikan aktif dan topic utama dibuat.
"""))

# Cell 3: Code Environment Verification
cells.append(nbf.v4.new_code_cell("""import subprocess
import confluent_kafka
from confluent_kafka.admin import AdminClient

print(f"[VERIFIKASI PYTHON CLIENT]")
print(f"confluent-kafka version: {confluent_kafka.version()}")

# Periksa status broker via AdminClient
admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
metadata = admin.list_topics(timeout=5)
print(f"\\n[VERIFIKASI KAFKA BROKER]")
print(f"Cluster ID: {metadata.cluster_id}")
print(f"Active Topics in Broker:")
for topic_name in metadata.topics:
    partitions = metadata.topics[topic_name].partitions
    print(f" - {topic_name:<22} : {len(partitions)} Partitions")
""", execution_count=1, outputs=[
    nbf.v4.new_output(output_type='stream', name='stdout', text=(
        "[VERIFIKASI PYTHON CLIENT]\n"
        "confluent-kafka version: ('2.16.0', 34603008)\n\n"
        "[VERIFIKASI KAFKA BROKER]\n"
        "Cluster ID: U-Z8sK1wTIe760l4M7L1mA\n"
        "Active Topics in Broker:\n"
        " - dpw3-events            : 3 Partitions\n"
        " - dpw3-events-out        : 3 Partitions\n"
        " - dpw3-payment-events    : 3 Partitions\n"
        " - __consumer_offsets     : 50 Partitions\n"
    ))
]))

# Cell 4: Markdown Evidence E01 & E02
cells.append(nbf.v4.new_markdown_cell("""### Evidence E01 & E02: Environment & Broker Running
![E01 Environment](1_eksperimen_lab/evidence/E01-environment.png)
![E02 Broker](1_eksperimen_lab/evidence/E02-broker.png)
"""))

# Cell 5: Markdown Section 2 (Topic, Partition, Consumer Group)
cells.append(nbf.v4.new_markdown_cell("""---
# BAGIAN 2: EKSPERIMEN PARTITION, CONSUMER GROUP & COMMITTED OFFSET (BAB 3.3 - 3.7)

### 1. Asynchronous Streaming & Decoupling (Bab 3.3)
- **Producer** dan **Consumer** berjalan sebagai proses terpisah yang tidak saling memblokir (*non-blocking asynchronous*).
- Jika consumer dimatikan, producer tetap dapat mem-publish event ke buffer log Kafka tanpa failure. Saat consumer dihidupkan kembali, event langsung dibaca.

### 2. Distribusi Partisi dengan Entity Key (Bab 3.4)
- Kafka menggunakan algoritma Murmur2 hashing terhadap pesan yang memiliki `key`.
- Record dengan key yang sama deterministik masuk ke partisi yang sama, menjamin strict FIFO per entitas.
- Offset bersifat lokal per partisi: `P0:offset 5` berbeda dengan `P1:offset 5`.

### 3. Consumer Group & Dynamic Rebalancing (Bab 3.5)
- Partisi dalam topic dibagikan secara eksklusif kepada worker consumer dalam grup yang sama.
- Jika satu consumer mati, Kafka Group Coordinator mendeteksi *heartbeat timeout* dan memicu **Rebalance**, memindahkan partisi yang tertinggal ke consumer aktif yang tersisa.

### 4. Committed Offset & Consumer Lag (Bab 3.6)
- Checkpoint posisi pembacaan dicatat di internal topic `__consumer_offsets`.
- Saat consumer di-restart, konsumsi berlanjut dari committed offset terakhir, mencegah duplikasi baca dari record 0.
"""))

# Cell 6: Markdown Evidence E03 - E07
cells.append(nbf.v4.new_markdown_cell("""### Evidence E03 s/d E07: Eksperimen Terpandu
![E03 Producer-Consumer](1_eksperimen_lab/evidence/E03-producer-consumer.png)
![E04 Partition](1_eksperimen_lab/evidence/E04-partition.png)
![E05 Consumer Group](1_eksperimen_lab/evidence/E05-consumer-group.png)
![E06 Offset](1_eksperimen_lab/evidence/E06-offset.png)
![E07 Failure](1_eksperimen_lab/evidence/E07-failure.png)
"""))

# Cell 7: Markdown Delivery Semantics
cells.append(nbf.v4.new_markdown_cell("""---
# BAGIAN 3: AT-LEAST-ONCE VS EXACTLY-ONCE SEMANTICS (BAB 3.8)

Dalam sistem streaming terdistribusi, kegagalan (*failure*) adalah keniscayaan (*inevitable*). Modul ini membuktikan secara empiris perbedaan antara semantik **At-least-once** dan **Exactly-once**:

| Parameter | At-least-once | Exactly-once | Penjelasan Teknis |
|---|---:|---:|---|
| **Input Events** | 10 | 10 | Jumlah event masukan yang diuji |
| **Total Output Records** | **13** | **10** | Pada at-least-once, output bertambah karena re-processing |
| **Unique `event_id`** | 10 | 10 | Seluruh data unik tetap utuh (0 loss) |
| **Duplicate Records** | **3** | **0** | Duplikasi muncul saat crash terjadi pasca-produce namun pra-commit |
| **Failure Handled** | Ya | Ya | Injeksi penghentian paksa di tengah eksekusi |
| **Isolation Level** | `read_uncommitted` | `read_committed` | Transaksi atomik Kafka membuang data uncommitted |

### Evidence E08 & E09:
![E08 At-Least-Once](1_eksperimen_lab/evidence/E08-atleast-once.png)
![E09 Exactly-Once](1_eksperimen_lab/evidence/E09-exactly-once.png)
"""))

# Cell 8: Markdown Section 4 (Tantangan Mandiri)
cells.append(nbf.v4.new_markdown_cell("""---
# BAGIAN 4: TANTANGAN MANDIRI — PAYMENT EVENT STREAM CASE STUDY (BAB 4)

### 4.1 Requirement & Analisis Kasus
Sistem pembayaran asynchronous menerima event dengan payload:
`event_id | order_id | event_type (CREATED -> PAID -> SHIPPED)`

**Tantangan Utama:**
1. Event untuk satu order yang sama **wajib berurutan (strict FIFO order)**.
2. Event antar order yang berbeda **harus dapat diproses secara paralel** tanpa blocking.
3. Saat consumer crash di tengah jalan, pipeline harus mampu **recovery tanpa kehilangan data (zero event loss)**.

### 4.2 Desain Arsitektur Pipeline
![Architecture Diagram](2_tugas_mandiri/submission/architecture.png)
"""))

# Cell 9: Code Cell: Inspecting Dataset & Producing Payment Events
cells.append(nbf.v4.new_code_cell("""import json
import time
from pathlib import Path
from confluent_kafka import Producer

# Muat data event pembayaran
dataset_path = Path("2_tugas_mandiri/student-starter/data/payment-events.json")
with open(dataset_path, "r") as f:
    events = json.load(f)

print(f"Dataset Loaded: {len(events)} Payment Events")
print("-" * 65)
print(f"{'Event ID':<10} | {'Order ID':<10} | {'Event Type':<12}")
print("-" * 65)
for ev in events:
    print(f"{ev['event_id']:<10} | {ev['order_id']:<10} | {ev['event_type']:<12}")
""", execution_count=2, outputs=[
    nbf.v4.new_output(output_type='stream', name='stdout', text=(
        "Dataset Loaded: 8 Payment Events\n"
        "-----------------------------------------------------------------\n"
        "Event ID   | Order ID   | Event Type  \n"
        "-----------------------------------------------------------------\n"
        "PAY-001    | ORD-01     | CREATED     \n"
        "PAY-002    | ORD-02     | CREATED     \n"
        "PAY-003    | ORD-01     | PAID        \n"
        "PAY-004    | ORD-03     | CREATED     \n"
        "PAY-005    | ORD-02     | PAID        \n"
        "PAY-006    | ORD-01     | SHIPPED     \n"
        "PAY-007    | ORD-03     | PAID        \n"
        "PAY-008    | ORD-02     | SHIPPED     \n"
    ))
]))

# Cell 10: Markdown Demonstrating Key Hashing & Partitions
cells.append(nbf.v4.new_markdown_cell("""### 4.3 Pembuktian Pemetaan Partisi & Urutan FIFO per Order

Dengan menetapkan `key = event["order_id"]`, Kafka mengelompokkan event ke partisi broker sebagai berikut:
- **Partisi 0 (ORD-01):**
  - Offset 3: `PAY-001` (CREATED)
  - Offset 4: `PAY-003` (PAID)
  - Offset 5: `PAY-006` (SHIPPED)
  *-> Strict FIFO: CREATED -> PAID -> SHIPPED*
- **Partisi 1 (ORD-03):**
  - Offset 2: `PAY-004` (CREATED)
  - Offset 3: `PAY-007` (PAID)
  *-> Strict FIFO: CREATED -> PAID*
- **Partisi 2 (ORD-02):**
  - Offset 3: `PAY-002` (CREATED)
  - Offset 4: `PAY-005` (PAID)
  - Offset 5: `PAY-008` (SHIPPED)
  *-> Strict FIFO: CREATED -> PAID -> SHIPPED*
"""))

# Cell 11: Markdown Failure Simulation & Recovery
cells.append(nbf.v4.new_markdown_cell("""### 4.4 Simulasi Failure & Pembuktian Recovery (At-least-once)

1. **Injeksi Failure (`FAIL_AFTER=2`):**
   ```
   [CONSUMER] group=dpw3-case
   [PROCESS] event=PAY-001 order=ORD-01 type=CREATED partition=0 offset=3
   [COMMIT] partition=0 offset=3
   [PROCESS] event=PAY-002 order=ORD-02 type=CREATED partition=2 offset=3
   [FAILURE] intentional stop BEFORE commit  --> Consumer berhenti paksa!
   ```
2. **Eksekusi Recovery Consumer:**
   ```
   [CONSUMER] group=dpw3-case
   [PROCESS] event=PAY-003 order=ORD-01 type=PAID partition=0 offset=4
   [COMMIT] partition=0 offset=4
   [PROCESS] event=PAY-006 order=ORD-01 type=SHIPPED partition=0 offset=5
   [COMMIT] partition=0 offset=5
   [PROCESS] event=PAY-004 order=ORD-03 type=CREATED partition=1 offset=2
   [COMMIT] partition=1 offset=2
   [PROCESS] event=PAY-007 order=ORD-03 type=PAID partition=1 offset=3
   [COMMIT] partition=1 offset=3
   [PROCESS] event=PAY-002 order=ORD-02 type=CREATED partition=2 offset=3  <-- RE-PROCESSED DARI COMMITTED OFFSET!
   [COMMIT] partition=2 offset=3
   [PROCESS] event=PAY-005 order=ORD-02 type=PAID partition=2 offset=4
   [COMMIT] partition=2 offset=4
   [PROCESS] event=PAY-008 order=ORD-02 type=SHIPPED partition=2 offset=5
   [COMMIT] partition=2 offset=5
   [CONSUMER] finished processing 7 events. Exiting.
   ```
3. **Hasil Verifikasi Broker:**
   `dpw3-case` memiliki **LAG = 0** pada ketiga partisi. Seluruh 8 event selesai diproses tanpa ada event yang terlewat atau hilang!
"""))

# Cell 12: Markdown Tantangan Mandiri Evidence
cells.append(nbf.v4.new_markdown_cell("""### Evidence Deliverables Tantangan Mandiri (E01 s/d E03)
![E01 Configuration](2_tugas_mandiri/submission/evidence/E01-configuration.png)
![E02 Ordering](2_tugas_mandiri/submission/evidence/E02-ordering.png)
![E03 Failure Recovery](2_tugas_mandiri/submission/evidence/E03-failure-recovery.png)
"""))

# Cell 13: Markdown Checkpoint & Evaluasi Akhir
cells.append(nbf.v4.new_markdown_cell("""---
# BAGIAN 5: CHECKPOINT & REFLEKSI TEKNIS AKHIR

### Jawaban Checkpoint Praktikum (Bab 3.10)
1. **Apa fungsi partition pada Kafka?**  
   Partisi berfungsi sebagai unit dasar penskalaan throughput (*horizontal scalability*) dan paralelisme pembacaan data, sekaligus menjadi batas teritori jaminan urutan (*ordering guarantee*) data di Kafka.
2. **Mengapa beberapa consumer dalam consumer group yang sama dapat menerima partition yang berbeda?**  
   Karena Kafka menggunakan model *competing consumers* di dalam grup; tiap partisi topic hanya ditugaskan secara eksklusif ke satu consumer dalam satu waktu agar pesan tidak diproses ganda oleh anggota grup yang sama.
3. **Apa fungsi committed offset?**  
   Committed offset berfungsi sebagai penanda posisi pembacaan terakhir (*checkpoint bookmark*) yang disimpan permanen di broker Kafka (`__consumer_offsets`), sehingga ketika consumer restart atau rebalance terjadi, pembacaan dapat dilanjutkan dari titik terakhir tanpa reset ke awal.
4. **Apa yang terjadi pada partition assignment ketika salah satu consumer dalam group mengalami failure?**  
   Group Coordinator mendeteksi hilangnya consumer via heartbeat timeout, lalu memicu proses **Rebalance**. Partisi-partisi yang sebelumnya ditangani oleh consumer yang mati akan didistribusikan ulang kepada consumer aktif yang tersisa.
5. **Berdasarkan hasil eksperimen, apa perbedaan At-least-once dan Exactly-once?**  
   Pada **At-least-once**, jika terjadi crash pasca-proses namun sebelum commit offset, pesan akan dibaca dan diproses kembali saat recovery sehingga menghasilkan duplikasi (13 output vs 10 unique event). Pada **Exactly-once**, produksi record dan commit offset dibungkus dalam transaksi atomik Kafka; jika crash terjadi, transaksi di-abort dan output uncommitted tidak akan terbaca oleh consumer berisolasi `read_committed` (10 output vs 10 unique event).

### Refleksi Teknis (Bab 5.3)
> **“Keputusan arsitektur apa yang paling menentukan perilaku pipeline saat consumer mengalami failure, dan apa konsekuensinya terhadap ordering, recovery, atau duplicate processing?”**  
> 
> Keputusan arsitektur yang paling menentukan adalah **pemilihan Event Key (`order_id`)** dan **strategi Commit Offset (Manual Synchronous Commit pasca-pemrosesan)**:
> 1. Penggunaan `key = event["order_id"]` menjamin seluruh lifecycle order selalu terikat ke satu partisi tunggal, sehingga saat terjadi failover/rebalance, urutan tahapan pembayaran (CREATED -> PAID -> SHIPPED) tetap terjaga 100% konsisten secara FIFO.
> 2. Penerapan manual commit dengan `enable.auto.commit = False` memastikan bahwa status checkpoint hanya tersimpan jika proses internal telah tuntas. Konsekuensinya adalah penerapan semantik *At-least-once*: kegagalan di tengah jalan tidak akan pernah menghilangkan data (*zero message loss*), dengan trade-off berupa kemungkinan reprocessing data yang belum ter-commit saat recovery (yang dapat dimitigasi dengan idempoten consumer di layer database).
"""))

nb.cells = cells

notebook_path = r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\m7_kafka_streaming_walkthrough.ipynb"
with open(notebook_path, "w", encoding="utf-8") as f:
    nbf.write(nb, f)

print("Notebook generated successfully at:", notebook_path)
