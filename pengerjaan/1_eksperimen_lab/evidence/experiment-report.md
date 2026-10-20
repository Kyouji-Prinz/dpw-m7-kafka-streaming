# Experiment Report — DPW Modul 3

## 1. Partition Analysis
Evidence:
E04-partition.png (Pengiriman 15 event dengan `--key-mode entity` ke topic `dpw3-events`).

Observation:
Event didistribusikan ke tiga partisi berbeda secara seimbang (Partition 0 menerima offset 0-5, Partition 1 menerima offset 0-8, Partition 2 menerima offset 0-9). Record dengan entity key yang sama selalu masuk ke partisi yang identik secara konsisten.

Conclusion:
Satu Kafka topic dapat dipecah menjadi beberapa partisi paralel. Posisi offset bersifat lokal per partisi, bukan global untuk seluruh topic. Offset 5 pada partisi 0 merepresentasikan record yang sama sekali berbeda dengan offset 5 pada partisi 1.

## 2. Consumer Group Analysis
Evidence:
E05-consumer-group.png (Dua consumer C1 dan C2 dalam grup `dpw3-group`, diikuti penghentian C2).

Observation:
Saat C1 dan C2 berjalan aktif bersama, 3 partisi topic dibagi di antara keduanya (misal C1 memegang partisi 0 dan 1, C2 memegang partisi 2). Ketika C2 dimatikan dengan SIGINT (Ctrl+C), Kafka mendeteksi berkurangnya anggota grup dan memicu proses **Rebalance**. Partisi 2 yang sebelumnya dipegang oleh C2 secara otomatis dialihkan ke C1.

Conclusion:
Consumer Group memungkinkan penskalaan horizontal konsumsi streaming secara paralel. Rebalance menjamin kontinuitas pemrosesan data saat ada consumer yang bergabung atau keluar/gagal.

## 3. Offset Analysis
Evidence:
E06-offset.png (Eksekusi consumer grup `dpw3-offset` dengan `--stop-after 5`, diikuti inspeksi dan re-run).

Observation:
Consumer membaca 5 event pertama, melakukan commit offset pada posisi 5, lalu berhenti. Saat perintah `kafka-consumer-groups.sh` dijalankan, tertera `CURRENT-OFFSET = 5`, `LOG-END-OFFSET = 15`, dan `LAG = 10`. Ketika consumer dijalankan kembali tanpa reset flag, consumer langsung melanjutkan dari record ke-6 (offset 5) tanpa membaca ulang dari awal (offset 0).

Conclusion:
Committed offset yang dicatat dalam internal topic `__consumer_offsets` berfungsi sebagai checkpoint status konsumsi bagi consumer group. Hal ini memungkinkan pemrosesan data bersifat stateful dan dapat dilanjutkan secara akurat setelah consumer berhenti.

## 4. Failure Analysis
Evidence:
E07-failure.png (Consumer C1 gagal dengan `--fail-after 3` dalam grup `dpw3-failure`).

Observation:
C1 mati mendadak saat streaming sedang berjalan. Consumer C2 yang berada dalam satu grup tetap aktif, dan setelah heartbeat timeout terlampaui, Kafka melakukan rebalance dan menugaskan seluruh sisa partisi ke C2.

Diagnosis:
Kegagalan salah satu node consumer mengubah keanggotaan (membership) grup. Tanpa koordinasi group rebalance, partisi yang ditangani oleh consumer yang mati akan terbengkalai (stuck).

Fix:
Menjalankan instance pengganti `C1-recovery` atau membiarkan Kafka menugaskan kembali partisi ke consumer yang masih hidup dalam grup.

Verification:
Output `kafka-consumer-groups.sh` menunjukkan consumer group kembali stabil dengan anggota aktif, dan seluruh event berhasil diproses hingga LAG mencapai 0.

## 5. At-least-once vs Exactly-once
| Parameter | At-least-once | Exactly-once |
|---|---:|---:|
| Input event | 10 | 10 |
| Total output | 13 | 10 |
| Unique event_id | 10 | 10 |
| Duplicate output | 3 | 0 |
| Failure terjadi | Ya (Post-produce, pre-commit) | Ya (Pre-commit transaction) |
| Output transaction committed | Tidak terisolasi (Non-atomic) | Terisolasi (Atomic Read Committed) |

## 6. Technical Reflection
Apa hubungan Partition, Consumer Group, Offset, dan Failure dalam menentukan perilaku sistem streaming?

**Refleksi Teknis:**
Keempat komponen tersebut membentuk fondasi arsitektur data streaming modern yang terdistribusi dan toleran terhadap kesalahan (*fault-tolerant*):
1. **Partition** adalah unit dasar penskalaan data dan paralelisme throughput, sekaligus batas dari jaminan urutan data (*ordering boundary*).
2. **Consumer Group** adalah unit komputasi paralel yang membagi partisi-partisi tersebut ke sejumlah worker consumer yang bekerja secara independen.
3. **Offset** adalah penanda kemajuan (*checkpoint bookmark*) yang mencatat sejauh mana setiap worker telah memproses data pada masing-masing partisi.
4. Ketika **Failure** terjadi pada consumer, mekanisme **Rebalance** menggunakan **Offset** yang telah di-commit sebagai titik awal bagi worker pengganti untuk melanjutkan konsumsi. Pemilihan strategi commit offset menentukan semantik pengiriman: commit sebelum proses berisiko *at-most-once* (kehilangan data), sedangkan commit setelah proses menghasilkan *at-least-once* (data aman namun berpotensi duplikasi saat recovery jika tidak diimbangi idempotensi atau transaksi exactly-once).
