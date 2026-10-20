import docx
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from pathlib import Path

def create_report():
    doc = docx.Document()
    
    # Page setup - Margins
    for section in doc.sections:
        section.top_margin = Inches(1.0)
        section.bottom_margin = Inches(1.0)
        section.left_margin = Inches(1.0)
        section.right_margin = Inches(1.0)

    # Styles
    style_normal = doc.styles['Normal']
    style_normal.font.name = 'Arial'
    style_normal.font.size = Pt(10.5)
    style_normal.font.color.rgb = RGBColor(0x1e, 0x29, 0x3b) # Slate 800

    # Header / Title
    p_title = doc.add_paragraph()
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run_title = p_title.add_run("LAPORAN PRAKTIKUM & TANTANGAN MANDIRI — MODUL 3 (M7)")
    run_title.bold = True
    run_title.font.size = Pt(16)
    run_title.font.color.rgb = RGBColor(0x0f, 0x17, 0x2a)

    p_sub = doc.add_paragraph()
    p_sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run_sub = p_sub.add_run("Topik: Data Ingestion – Event-Driven & Streaming Architecture dengan Apache Kafka")
    run_sub.font.size = Pt(12)
    run_sub.bold = True
    run_sub.font.color.rgb = RGBColor(0x02, 0x84, 0xc7)

    # Student Info Table
    p_meta = doc.add_paragraph()
    p_meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_meta.paragraph_format.space_after = Pt(14)
    run_meta = p_meta.add_run("Nama: Udi Adhahaska Arta Ananta   |   NRP: 5222600052   |   Kelas: 3 D4 Sains Data Terapan")
    run_meta.font.size = Pt(10)
    run_meta.italic = True
    run_meta.font.color.rgb = RGBColor(0x47, 0x55, 0x69)

    doc.add_heading("1. Ringkasan Eksekutif", level=1)
    p_exec = doc.add_paragraph(
        "Praktikum Modul 3 (M7) bertujuan untuk memahami dan mempraktikkan perancangan data ingestion asinkron "
        "berbasis arsitektur Event-Driven menggunakan Apache Kafka, Docker Compose, dan Python Client (confluent-kafka). "
        "Eksperimen terbagi menjadi dua capaian utama: (1) Eksperimen Terpandu untuk membuktikan konsep decoupling, "
        "partisi, consumer group rebalancing, checkpoint committed offset, serta komparasi empiris At-least-once vs Exactly-once; "
        "dan (2) Tantangan Mandiri (Payment Event Stream) untuk menjamin pemrosesan berurutan (strict FIFO) per order_id, "
        "paralelisasi antar order, dan toleransi kegagalan (fault recovery) tanpa kehilangan pesan."
    )
    p_exec.paragraph_format.line_spacing = 1.15

    # Section 2: Hasil Eksperimen Terpandu
    doc.add_heading("2. Hasil Eksperimen Terpandu (Bab 3)", level=1)

    sections = [
        ("2.1 Verifikasi Environment & Broker (E01 & E02)",
         "Verifikasi membuktikan Docker 28.5.1, Docker Compose v2.40.3, Python 3.12.8, dan confluent-kafka 2.16.0 "
         "berfungsi sempurna. Container broker Kafka 'dpw3-kafka' aktif pada port 9092 dengan topic 'dpw3-events' (3 partisi).",
         [
             r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\1_eksperimen_lab\evidence\E01-environment.png",
             r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\1_eksperimen_lab\evidence\E02-broker.png"
         ]),
        ("2.2 Producer-Consumer Asynchronous & Decoupling (E03)",
         "Producer dan Consumer berjalan sebagai proses independen. Ketika consumer dihentikan, producer tetap berhasil mengirim pesan ke Kafka buffer tanpa failure. Saat consumer dihidupkan kembali, pesan dibaca tanpa ada data yang hilang.",
         [r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\1_eksperimen_lab\evidence\E03-producer-consumer.png"]),
        ("2.3 Eksperimen Partisi & Key Hashing (E04)",
         "Pengiriman 15 event dengan entity key terdistribusi ke partisi 0 (6 event), partisi 1 (9 event), dan partisi 2 (10 event). Record dengan key yang sama selalu masuk ke partisi yang identik secara deterministik, membuktikan bahwa offset bersifat independen per partisi.",
         [r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\1_eksperimen_lab\evidence\E04-partition.png"]),
        ("2.4 Consumer Group & Dynamic Rebalancing (E05)",
         "Dua consumer (C1 dan C2) dalam grup 'dpw3-group' membagi beban partisi. Saat C2 dimatikan (failure simulation), Kafka mendeteksi perubahan membership dan memicu Rebalance sehingga C1 otomatis mengambil alih partisi yang ditinggalkan C2.",
         [r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\1_eksperimen_lab\evidence\E05-consumer-group.png"]),
        ("2.5 Committed Offset & Consumer Lag (E06)",
         "Consumer dijalankan dengan parameter '--stop-after 5'. Broker mencatat CURRENT-OFFSET = 5 dan LAG = 10. Saat consumer dijalankan kembali, pembacaan berlanjut dari offset 5 tanpa membaca ulang dari record awal (offset 0).",
         [r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\1_eksperimen_lab\evidence\E06-offset.png"]),
        ("2.6 Failure Injection & Recovery (E07)",
         "Injeksi crash pada consumer C1 setelah 3 event. Consumer group 'dpw3-failure' berhasil pulih melalui rebalance dan consumer pengganti melanjutkan sisa pesan tanpa kebuntuan partisi.",
         [r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\1_eksperimen_lab\evidence\E07-failure.png"]),
        ("2.7 Komparasi Semantik: At-least-once vs Exactly-once (E08 & E09)",
         "Pengujian empiris membuktikan bahwa crash setelah produksi output pada At-least-once menghasilkan duplikasi (13 output dari 10 unique events) karena offset belum ter-commit. Sebaliknya, pada Exactly-once dengan isolasi read_committed, transaksi uncommitted dibatalkan sehingga dihasilkan tepat 10 output tanpa duplikasi sama sekali.",
         [
             r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\1_eksperimen_lab\evidence\E08-atleast-once.png",
             r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\1_eksperimen_lab\evidence\E09-exactly-once.png"
         ])
    ]

    for title, desc, img_paths in sections:
        doc.add_heading(title, level=2)
        p = doc.add_paragraph(desc)
        p.paragraph_format.line_spacing = 1.15
        for img_path in img_paths:
            if Path(img_path).exists():
                doc.add_picture(img_path, width=Inches(6.0))
                doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
                p_caption = doc.add_paragraph(f"Gambar: Bukti Eksekusi {Path(img_path).name}")
                p_caption.alignment = WD_ALIGN_PARAGRAPH.CENTER
                p_caption.runs[0].font.size = Pt(8.5)
                p_caption.runs[0].font.italic = True
                p_caption.runs[0].font.color.rgb = RGBColor(0x64, 0x74, 0x8b)

    # Table for Semantics
    doc.add_heading("Tabel Perbandingan Semantik Pengiriman Pesan", level=3)
    table = doc.add_table(rows=6, cols=3)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.style = 'Light Shading Accent 1'

    headers = ["Parameter", "At-least-once", "Exactly-once"]
    for i, h in enumerate(headers):
        cell = table.cell(0, i)
        cell.text = h
        cell.paragraphs[0].runs[0].font.bold = True

    data_rows = [
        ("Input event", "10", "10"),
        ("Total output", "13", "10"),
        ("Unique event_id", "10", "10"),
        ("Duplicate output", "3", "0"),
        ("Karakteristik Transaksi", "Non-atomic (Post-produce failure)", "Atomic Read Committed (Tx Aborted)")
    ]

    for row_idx, row_data in enumerate(data_rows, start=1):
        for col_idx, text in enumerate(row_data):
            table.cell(row_idx, col_idx).text = text

    # Section 3: Tantangan Mandiri
    doc.add_heading("3. Hasil Tantangan Mandiri — Payment Event Stream (Bab 4)", level=1)
    p_mandiri = doc.add_paragraph(
        "Tantangan Mandiri menguji perancangan pipeline event streaming untuk transaksi pembayaran dengan kriteria:\n"
        "1. Strict Ordering per Order: Event untuk order yang sama (CREATED -> PAID -> SHIPPED) wajib diproses berurutan.\n"
        "2. Paralelisasi Antar Order: Event order berbeda dapat diproses bersamaan antar partisi.\n"
        "3. Fault Tolerance & Zero Loss: Kegagalan consumer di tengah jalan tidak boleh menghilangkan event pembayaran."
    )
    p_mandiri.paragraph_format.line_spacing = 1.15

    doc.add_heading("3.1 Diagram Arsitektur Pipeline", level=2)
    arch_img = r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\2_tugas_mandiri\submission\architecture.png"
    if Path(arch_img).exists():
        doc.add_picture(arch_img, width=Inches(6.2))
        doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
        p_cap = doc.add_paragraph("Gambar: Arsitektur Pemrosesan Payment Stream, Partisi Kafka, & Fault Recovery")
        p_cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p_cap.runs[0].font.size = Pt(8.5)
        p_cap.runs[0].font.italic = True

    doc.add_heading("3.2 Keputusan Arsitektur & Analisis Teknis", level=2)
    p_dec = doc.add_paragraph(
        "• Partisi: 3 partisi pada topic 'dpw3-payment-events' untuk membagi beban order dan memungkinkan konkurensi antar order.\n"
        "• Consumer Group: 'dpw3-case' sebagai koordinator assignment partisi dan pelacakan offset.\n"
        "• Event Key / Ordering: key = event['order_id']. Hashing Murmur2 Kafka menjamin semua event dengan order_id yang sama deterministik masuk ke partisi yang identik.\n"
        "• Offset / Commit: enable.auto.commit = False dengan synchronous commit manual setelah pemrosesan record berhasil.\n"
        "• Delivery Semantics: At-least-once delivery menjamin zero message loss saat worker mengalami crash."
    )
    p_dec.paragraph_format.line_spacing = 1.15

    doc.add_heading("3.3 Bukti Eksekusi (Evidence E01 s/d E03)", level=2)
    mandiri_evs = [
        ("Bukti E01 — Konfigurasi Topic, Partisi, dan Consumer Group",
         r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\2_tugas_mandiri\submission\evidence\E01-configuration.png"),
        ("Bukti E02 — Pembuktian Strict Ordering per Order_ID",
         r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\2_tugas_mandiri\submission\evidence\E02-ordering.png"),
        ("Bukti E03 — Simulasi Crash (FAIL_AFTER=2) dan Pemulihan (Recovery)",
         r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\2_tugas_mandiri\submission\evidence\E03-failure-recovery.png")
    ]
    for ev_title, ev_path in mandiri_evs:
        doc.add_heading(ev_title, level=3)
        if Path(ev_path).exists():
            doc.add_picture(ev_path, width=Inches(6.0))
            doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER

    # Section 4: Refleksi Teknis & Checkpoint
    doc.add_heading("4. Checkpoint Evaluasi & Refleksi Teknis", level=1)
    
    qa = [
        ("Apa fungsi partition pada Kafka?", 
         "Partisi adalah unit fundamental skalabilitas horizontal throughput Kafka. Partisi memungkinkan pembagian beban data ke banyak node dan consumer, sekaligus menetapkan batas jaminan urutan data (strict FIFO hanya dijamin per partisi)."),
        ("Mengapa beberapa consumer dalam consumer group yang sama menerima partition yang berbeda?",
         "Model competing consumers Kafka menetapkan pembagian eksklusif partisi kepada anggota grup agar pesan tidak dikonsumsi ganda oleh consumer internal dalam satu grup yang sama."),
        ("Apa fungsi committed offset?",
         "Committed offset mencatat checkpoint posisi konsumsi terakhir pada internal topic '__consumer_offsets'. Posisi ini memungkinkan consumer melanjutkan pembacaan data secara akurat saat terjadi crash, restart, atau rebalance."),
        ("Apa yang terjadi pada partition assignment ketika salah satu consumer mengalami failure?",
         "Koordinator grup Kafka mendeteksi kegagalan melalui heartbeat timeout dan memicu Rebalance. Partisi yang ditinggalkan oleh consumer yang mati akan dialihkan secara otomatis ke consumer aktif yang tersisa."),
        ("Berdasarkan hasil eksperimen, apa perbedaan At-least-once dan Exactly-once?",
         "Pada At-least-once, jika kegagalan terjadi sebelum offset di-commit, event akan diproses ulang saat recovery sehingga berpotensi menghasilkan duplikasi output. Pada Exactly-once, produksi pesan dan commit offset diikat dalam transaksi atomik; jika terjadi kegagalan, transaksi dibatalkan sehingga tidak ada duplikasi data yang terlihat oleh consumer berisolasi read_committed.")
    ]

    for q, a in qa:
        p_q = doc.add_paragraph()
        p_q.add_run(f"Pertanyaan: {q}").bold = True
        p_a = doc.add_paragraph(f"Jawaban: {a}")
        p_a.paragraph_format.line_spacing = 1.15
        p_a.paragraph_format.space_after = Pt(6)

    doc.add_heading("Refleksi Teknis (Bab 5.3)", level=2)
    p_ref = doc.add_paragraph(
        "Keputusan arsitektur yang paling menentukan perilaku pipeline adalah pemilihan Event Key ('order_id') "
        "dan strategi Commit Offset manual pasca-pemrosesan. Pemilihan key berbasis order_id menjamin integritas urutan status pembayaran "
        "(CREATED -> PAID -> SHIPPED) tetap terjaga 100% konsisten pada partisi yang sama meskipun terjadi rebalancing atau failover. "
        "Sementara itu, strategi manual synchronous commit dengan enable.auto.commit = False memberikan jaminan At-least-once: "
        "kegagalan sistem tidak akan pernah menghilangkan data event pembayaran, dengan konsekuensi logis perlunya penanganan idempoten "
        "pada consumer untuk mengantisipasi re-processing pasca-recovery."
    )
    p_ref.paragraph_format.line_spacing = 1.15

    output_doc_path = r"d:\PENS\1. MatKul\Semester 5\4. Data Processing Workflow\M7\pengerjaan\LAPORAN_LENGKAP_M7.docx"
    doc.save(output_doc_path)
    print("LAPORAN_LENGKAP_M7.docx successfully created at:", output_doc_path)

if __name__ == '__main__':
    create_report()
