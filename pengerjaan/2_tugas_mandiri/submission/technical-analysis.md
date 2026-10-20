# Technical Analysis

## Architecture Decision

Partition:
3 partisi pada topic `dpw3-payment-events` untuk membagi beban order dan memungkinkan pemrosesan paralel antar order yang berbeda.

Consumer Group:
`dpw3-case` digunakan sebagai entitas logis untuk mengelola keanggotaan consumer, koordinasi assignment partisi, dan tracking state offset.

Consumer:
1 worker consumer (dapat di-scale horizontal hingga 3 consumer aktif) yang membaca record dari seluruh partisi secara asinkron.

Event Key / Ordering:
`key = event["order_id"]`. Hashing partisi Kafka menjamin seluruh event dengan `order_id` yang sama masuk ke partisi yang identik secara deterministik, menjaga urutan FIFO (CREATED -> PAID -> SHIPPED).

Offset / Commit:
`enable.auto.commit = False` dengan synchronous commit manual (`consumer.commit(message=msg, asynchronous=False)`) per record setelah berhasil diproses.

Delivery Semantics:
At-least-once delivery.

Alasan:
Key berbasis `order_id` mempertahankan strict ordering per entitas order tanpa mengorbankan paralelisasi order lain. Manual commit setelah pemrosesan menjamin tidak ada data event pembayaran yang hilang saat terjadi crash, meskipun ada potensi reprocessing yang dapat ditangani secara idempoten.

## Failure Analysis

Action:
Menginjeksi kegagalan consumer dengan parameter `FAIL_AFTER=2` saat memproses event kedua (PAY-002) tepat sebelum offset di-commit, kemudian menjalankan recovery consumer.

Observation:
Consumer pertama berhenti seketika sebelum commit partisi 2 offset 3, lalu saat recovery dijalankan, event PAY-002 dibaca ulang dari offset 3 dan pemrosesan berlanjut hingga selesai 8 event.

Diagnosis:
Karena crash terjadi sebelum `consumer.commit()`, broker Kafka mempertahankan committed offset terakhir sehingga posisi konsumsi tidak melompati pesan yang belum terkonfirmasi.

Fix:
Menjalankan ulang instance consumer baru dalam group `dpw3-case` tanpa perlu intervensi modifikasi kode.

Verification:
Consumer berhasil memproses seluruh event dengan urutan FIFO terjaga pada tiap order (ORD-01: CREATED->PAID->SHIPPED, ORD-02: CREATED->PAID->SHIPPED, ORD-03: CREATED->PAID), dan perintah `kafka-consumer-groups.sh` mengonfirmasi LAG = 0 di ketiga partisi.
