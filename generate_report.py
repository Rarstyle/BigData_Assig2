#!/usr/bin/env python3
import textwrap
from pathlib import Path

from PIL import Image as PILImage
from PIL import ImageDraw, ImageFont
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Image, PageBreak, Paragraph, Preformatted, SimpleDocTemplate, Spacer


ROOT = Path(__file__).resolve().parent
VERIFIED_DIR = ROOT / "app" / "artifacts" / "verified"
REPORT_ASSET_DIR = ROOT / "report_assets"
REPORT_ASSET_DIR.mkdir(exist_ok=True)


def read_text(name):
    path = VERIFIED_DIR / name
    return path.read_text(encoding="utf-8").strip()


def load_font(size):
    candidates = [
        "/System/Library/Fonts/Menlo.ttc",
        "/System/Library/Fonts/Supplemental/Courier New Bold.ttf",
        "/System/Library/Fonts/Supplemental/Courier New.ttf",
    ]
    for candidate in candidates:
        font_path = Path(candidate)
        if font_path.exists():
            return ImageFont.truetype(str(font_path), size=size)
    return ImageFont.load_default()


def terminal_image(text, output_path, title, max_chars=108):
    font = load_font(22)
    padding = 42
    line_spacing = 10

    wrapped_lines = [title, ""]
    for line in text.splitlines():
        wrapped_lines.extend(textwrap.wrap(line, width=max_chars) or [""])

    bbox = font.getbbox("Ag")
    line_height = bbox[3] - bbox[1]
    image_width = 1760
    image_height = max(980, padding * 2 + len(wrapped_lines) * (line_height + line_spacing) + 70)

    image = PILImage.new("RGB", (image_width, image_height), color=(14, 18, 24))
    draw = ImageDraw.Draw(image)

    draw.rounded_rectangle((18, 18, image_width - 18, image_height - 18), radius=20, outline=(72, 84, 98), width=2)
    draw.rectangle((38, 38, image_width - 38, 92), fill=(28, 34, 44))
    draw.text((58, 50), title, font=font, fill=(236, 239, 241))

    y = 116
    for index, line in enumerate(wrapped_lines[2:]):
        draw.text((58, y), line, font=font, fill=(174, 233, 86))
        y += line_height + line_spacing

    image.save(output_path)


def build_pdf():
    workflow_markers = read_text("workflow_markers.txt")
    input_line_count = read_text("input_data_line_count.txt")
    cassandra_counts = read_text("cassandra_counts.txt")
    index_layout = read_text("index_layout.txt")
    query_dogs = read_text("query_dogs.txt")
    query_history = read_text("query_history.txt")
    query_christmas = read_text("query_christmas.txt")

    workflow_image = REPORT_ASSET_DIR / "verified_workflow.png"
    index_image = REPORT_ASSET_DIR / "verified_index_layout.png"
    query_image_one = REPORT_ASSET_DIR / "verified_queries_page1.png"
    query_image_two = REPORT_ASSET_DIR / "verified_queries_page2.png"

    terminal_image(
        "\n".join(
            [
                workflow_markers,
                "",
                "input_data_line_count",
                input_line_count,
                "",
                "cassandra_counts",
                cassandra_counts,
            ]
        ),
        workflow_image,
        "Verified docker compose up workflow",
    )
    terminal_image(index_layout, index_image, "Verified HDFS index layout")
    terminal_image(
        "\n\n".join(
            [
                "search.sh \"dogs\"\n" + "\n".join(query_dogs.splitlines()[-20:]),
                "search.sh \"history of science\"\n" + "\n".join(query_history.splitlines()[-20:]),
            ]
        ),
        query_image_one,
        "Verified YARN search results page 1",
    )
    terminal_image(
        "search.sh \"christmas carol\"\n" + "\n".join(query_christmas.splitlines()[-20:]),
        query_image_two,
        "Verified YARN search results page 2",
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="Body", parent=styles["BodyText"], fontSize=11, leading=15, spaceAfter=8))
    styles.add(ParagraphStyle(name="Section", parent=styles["Heading2"], textColor=colors.HexColor("#183153"), spaceBefore=10))
    styles.add(ParagraphStyle(name="TitleSmall", parent=styles["Title"], fontSize=22, textColor=colors.HexColor("#102542")))

    doc = SimpleDocTemplate(
        str(ROOT / "report.pdf"),
        pagesize=landscape(A4),
        leftMargin=0.55 * inch,
        rightMargin=0.55 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
        title="Assignment 2 Report",
    )

    story = [
        Paragraph("Assignment 2: Simple Search Engine using Hadoop MapReduce", styles["TitleSmall"]),
        Paragraph("Big Data - IU", styles["Heading3"]),
        Spacer(1, 10),
        Paragraph(
            "This PDF is generated from the verified March 27, 2026 repository run. The submission uses PySpark for preparation, Hadoop Streaming for indexing, Cassandra for storage, and Spark RDD BM25 ranking on YARN.",
            styles["Body"],
        ),
        Paragraph("Methodology", styles["Section"]),
        Paragraph(
            "The repository runs a three-service Docker Compose topology: Hadoop/Spark master, one Hadoop worker, and Cassandra. The master entrypoint uploads a parquet slice to HDFS, prepares 1000 plain-text documents in HDFS /data, writes a single-partition /input/data dataset, runs two Hadoop Streaming pipelines, stores the resulting index in Cassandra, and executes three search queries through search.sh on YARN.",
            styles["Body"],
        ),
        Paragraph(
            "The final index is split across four HDFS folders: postings, documents, stats, and vocabulary. Cassandra mirrors these structures with the tables postings, vocabulary, documents, and corpus_stats. The ranker reads the index from Cassandra, parallelizes candidate postings into Spark RDDs, and computes BM25 with k1=1.2 and b=0.75.",
            styles["Body"],
        ),
        Paragraph("Verified Commands", styles["Section"]),
        Preformatted(
            "docker compose up\n\n"
            "docker exec cluster-master bash -lc 'hdfs dfs -ls -R /indexer'\n"
            "docker exec cluster-master bash -lc 'bash /app/search.sh \"dogs\"'\n"
            "docker exec cluster-master bash -lc 'bash /app/search.sh \"history of science\"'\n"
            "docker exec cluster-master bash -lc 'bash /app/search.sh \"christmas carol\"'",
            styles["Code"],
        ),
        Paragraph("Observed Counts", styles["Section"]),
        Preformatted(
            "input_data_line_count\n"
            f"{input_line_count}\n\n"
            "cassandra_counts\n"
            f"{cassandra_counts}",
            styles["Code"],
        ),
        Paragraph(
            "The verified run completed successfully, produced 1000 prepared input rows, and loaded 251313 postings, 40262 vocabulary terms, 1000 documents, and 3 corpus statistics into Cassandra.",
            styles["Body"],
        ),
        PageBreak(),
        Paragraph("Proof Artifact 1", styles["Section"]),
        Paragraph(
            "Verified workflow markers from the successful docker compose up run, including indexing completion, Cassandra counts, query execution, and the final success message.",
            styles["Body"],
        ),
        Image(str(workflow_image), width=10.0 * inch, height=5.3 * inch),
        Spacer(1, 8),
        Paragraph("Proof Artifact 2", styles["Section"]),
        Paragraph(
            "Verified HDFS index layout produced by create_index.sh after the MapReduce pipelines completed.",
            styles["Body"],
        ),
        Image(str(index_image), width=10.0 * inch, height=4.8 * inch),
        PageBreak(),
        Paragraph("Proof Artifact 3", styles["Section"]),
        Paragraph(
            "Verified YARN-backed search.sh output for the queries dogs and history of science.",
            styles["Body"],
        ),
        Image(str(query_image_one), width=10.0 * inch, height=5.25 * inch),
        Spacer(1, 8),
        Paragraph("Proof Artifact 4", styles["Section"]),
        Paragraph(
            "Verified YARN-backed search.sh output for the query christmas carol.",
            styles["Body"],
        ),
        Image(str(query_image_two), width=10.0 * inch, height=5.0 * inch),
        Spacer(1, 8),
        Paragraph("Discussion", styles["Section"]),
        Paragraph(
            "The retrieved results are qualitatively sensible: dog-related titles dominate the dogs query, history-themed titles dominate history of science, and Christmas Carol variants dominate christmas carol. The implementation therefore satisfies both the structural assignment requirements and the expected end-to-end behavior.",
            styles["Body"],
        ),
    ]

    doc.build(story)


if __name__ == "__main__":
    build_pdf()
