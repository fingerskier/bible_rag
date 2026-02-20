"""Ingest a Bible CSV into the scripture table and generate chunks.

Usage:
    python ingest_csv.py <csv_file> [--version KJV]

The CSV must have columns: book, chapter, verse, text
(column names are case-insensitive; common aliases are supported).

Environment variables:
    DATABASE_URL  - PostgreSQL connection string
"""

import argparse
import csv
import os
import sys

import psycopg2
from psycopg2.extras import execute_values


DATABASE_URL = os.environ.get("DATABASE_URL")

BOOK_ORDER = [
    "GEN", "EXO", "LEV", "NUM", "DEU", "JOS", "JDG", "RUT",
    "1SA", "2SA", "1KI", "2KI", "1CH", "2CH", "EZR", "NEH",
    "EST", "JOB", "PSA", "PRO", "ECC", "SNG", "ISA", "JER",
    "LAM", "EZK", "DAN", "HOS", "JOL", "AMO", "OBA", "JON",
    "MIC", "NAH", "HAB", "ZEP", "HAG", "ZEC", "MAL",
    "MAT", "MRK", "LUK", "JHN", "ACT", "ROM", "1CO", "2CO",
    "GAL", "EPH", "PHP", "COL", "1TH", "2TH", "1TI", "2TI",
    "TIT", "PHM", "HEB", "JAS", "1PE", "2PE", "1JN", "2JN",
    "3JN", "JUD", "REV",
]

# Common alternative book abbreviations mapped to the canonical 3-letter codes
BOOK_ALIASES = {
    "genesis": "GEN", "gen": "GEN",
    "exodus": "EXO", "exo": "EXO", "exod": "EXO",
    "leviticus": "LEV", "lev": "LEV",
    "numbers": "NUM", "num": "NUM",
    "deuteronomy": "DEU", "deu": "DEU", "deut": "DEU",
    "joshua": "JOS", "jos": "JOS", "josh": "JOS",
    "judges": "JDG", "jdg": "JDG", "judg": "JDG",
    "ruth": "RUT", "rut": "RUT",
    "1samuel": "1SA", "1sa": "1SA", "1sam": "1SA",
    "2samuel": "2SA", "2sa": "2SA", "2sam": "2SA",
    "1kings": "1KI", "1ki": "1KI", "1kgs": "1KI",
    "2kings": "2KI", "2ki": "2KI", "2kgs": "2KI",
    "1chronicles": "1CH", "1ch": "1CH", "1chr": "1CH",
    "2chronicles": "2CH", "2ch": "2CH", "2chr": "2CH",
    "ezra": "EZR", "ezr": "EZR",
    "nehemiah": "NEH", "neh": "NEH",
    "esther": "EST", "est": "EST",
    "job": "JOB",
    "psalms": "PSA", "psalm": "PSA", "psa": "PSA", "pss": "PSA",
    "proverbs": "PRO", "pro": "PRO", "prov": "PRO",
    "ecclesiastes": "ECC", "ecc": "ECC", "eccl": "ECC",
    "songofsolomon": "SNG", "sng": "SNG", "song": "SNG", "sos": "SNG",
    "isaiah": "ISA", "isa": "ISA",
    "jeremiah": "JER", "jer": "JER",
    "lamentations": "LAM", "lam": "LAM",
    "ezekiel": "EZK", "ezk": "EZK", "eze": "EZK",
    "daniel": "DAN", "dan": "DAN",
    "hosea": "HOS", "hos": "HOS",
    "joel": "JOL", "jol": "JOL", "joe": "JOL",
    "amos": "AMO", "amo": "AMO",
    "obadiah": "OBA", "oba": "OBA", "obad": "OBA",
    "jonah": "JON", "jon": "JON",
    "micah": "MIC", "mic": "MIC",
    "nahum": "NAH", "nah": "NAH",
    "habakkuk": "HAB", "hab": "HAB",
    "zephaniah": "ZEP", "zep": "ZEP", "zeph": "ZEP",
    "haggai": "HAG", "hag": "HAG",
    "zechariah": "ZEC", "zec": "ZEC", "zech": "ZEC",
    "malachi": "MAL", "mal": "MAL",
    "matthew": "MAT", "mat": "MAT", "matt": "MAT",
    "mark": "MRK", "mrk": "MRK", "mar": "MRK",
    "luke": "LUK", "luk": "LUK",
    "john": "JHN", "jhn": "JHN", "joh": "JHN",
    "acts": "ACT", "act": "ACT",
    "romans": "ROM", "rom": "ROM",
    "1corinthians": "1CO", "1co": "1CO", "1cor": "1CO",
    "2corinthians": "2CO", "2co": "2CO", "2cor": "2CO",
    "galatians": "GAL", "gal": "GAL",
    "ephesians": "EPH", "eph": "EPH",
    "philippians": "PHP", "php": "PHP", "phil": "PHP",
    "colossians": "COL", "col": "COL",
    "1thessalonians": "1TH", "1th": "1TH", "1thess": "1TH",
    "2thessalonians": "2TH", "2th": "2TH", "2thess": "2TH",
    "1timothy": "1TI", "1ti": "1TI", "1tim": "1TI",
    "2timothy": "2TI", "2ti": "2TI", "2tim": "2TI",
    "titus": "TIT", "tit": "TIT",
    "philemon": "PHM", "phm": "PHM", "phile": "PHM",
    "hebrews": "HEB", "heb": "HEB",
    "james": "JAS", "jas": "JAS",
    "1peter": "1PE", "1pe": "1PE", "1pet": "1PE",
    "2peter": "2PE", "2pe": "2PE", "2pet": "2PE",
    "1john": "1JN", "1jn": "1JN", "1joh": "1JN",
    "2john": "2JN", "2jn": "2JN", "2joh": "2JN",
    "3john": "3JN", "3jn": "3JN", "3joh": "3JN",
    "jude": "JUD", "jud": "JUD",
    "revelation": "REV", "rev": "REV",
}


def normalize_book(raw: str) -> str:
    """Normalize a book name/abbreviation to canonical 3-letter code."""
    key = raw.strip().lower().replace(" ", "")
    if key in BOOK_ALIASES:
        return BOOK_ALIASES[key]
    upper = raw.strip().upper()
    if upper in BOOK_ORDER:
        return upper
    raise ValueError(f"Unknown book name: {raw!r}")


def detect_columns(headers: list[str]) -> dict[str, int]:
    """Map expected fields to column indices from CSV headers."""
    lower = [h.strip().lower() for h in headers]
    mapping = {}

    for alias in ("book", "book_name", "bookname", "b"):
        if alias in lower:
            mapping["book"] = lower.index(alias)
            break

    for alias in ("chapter", "chapter_number", "ch", "c"):
        if alias in lower:
            mapping["chapter"] = lower.index(alias)
            break

    for alias in ("verse", "verse_number", "v"):
        if alias in lower:
            mapping["verse"] = lower.index(alias)
            break

    for alias in ("text", "scripture", "content", "verse_text", "t"):
        if alias in lower:
            mapping["text"] = lower.index(alias)
            break

    missing = {"book", "chapter", "verse", "text"} - mapping.keys()
    if missing:
        raise ValueError(
            f"CSV is missing required columns: {missing}. "
            f"Found headers: {headers}"
        )
    return mapping


def init_schema(conn):
    """Create tables if they don't exist (runs schema.sql)."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("Schema initialized.")


def load_csv(path: str, version: str) -> list[tuple]:
    """Read a Bible CSV and return rows as (version, book, chapter, verse, text)."""
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        headers = next(reader)
        col = detect_columns(headers)

        for line_num, row in enumerate(reader, start=2):
            if not row or all(c.strip() == "" for c in row):
                continue
            try:
                book = normalize_book(row[col["book"]])
                chapter = int(row[col["chapter"]])
                verse = int(row[col["verse"]])
                text = row[col["text"]].strip()
            except (ValueError, IndexError) as e:
                print(f"  Warning: skipping line {line_num}: {e}")
                continue

            if not text:
                continue
            rows.append((version, book, chapter, verse, text))

    return rows


def insert_scripture(conn, rows: list[tuple]) -> int:
    """Bulk-insert scripture rows, skipping duplicates. Returns count inserted."""
    sql = """
        INSERT INTO scripture (version, book, chapter, verse, text)
        VALUES %s
        ON CONFLICT (version, book, chapter, verse) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def build_verse_chunks(conn, version: str):
    """Create one chunk per verse."""
    sql = """
        INSERT INTO chunks (version, chunk_type, start_scripture_id, end_scripture_id, label)
        SELECT version, 'verse', id, id, book || ' ' || chapter || ':' || verse
        FROM scripture
        WHERE version = %s
        ON CONFLICT (version, chunk_type, start_scripture_id, end_scripture_id) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, (version,))
        count = cur.rowcount
    conn.commit()
    print(f"  Verse chunks: {count}")


def build_chapter_chunks(conn, version: str):
    """Create one chunk per chapter."""
    sql = """
        INSERT INTO chunks (version, chunk_type, start_scripture_id, end_scripture_id, label)
        SELECT
            s.version,
            'chapter',
            MIN(s.id),
            MAX(s.id),
            s.book || ' ' || s.chapter
        FROM scripture s
        WHERE s.version = %s
        GROUP BY s.version, s.book, s.chapter
        ON CONFLICT (version, chunk_type, start_scripture_id, end_scripture_id) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, (version,))
        count = cur.rowcount
    conn.commit()
    print(f"  Chapter chunks: {count}")


def build_segment_chunks(conn, version: str, segment_size: int = 5):
    """Create overlapping N-verse segment chunks within each chapter."""
    with conn.cursor() as cur:
        # Fetch all scripture ids ordered by book order, chapter, verse
        cur.execute(
            """
            SELECT id, book, chapter, verse
            FROM scripture
            WHERE version = %s
            ORDER BY
                array_position(%s::varchar[], book),
                chapter,
                verse
            """,
            (version, BOOK_ORDER),
        )
        all_verses = cur.fetchall()

    # Group by (book, chapter)
    chapters: dict[tuple, list] = {}
    for sid, book, chapter, verse in all_verses:
        key = (book, chapter)
        chapters.setdefault(key, []).append(sid)

    chunk_rows = []
    for (book, chapter), ids in chapters.items():
        for i in range(0, len(ids), segment_size):
            segment = ids[i : i + segment_size]
            if len(segment) < 2:
                continue  # skip trivially small segments
            chunk_rows.append((
                version,
                "segment",
                segment[0],
                segment[-1],
                f"{book} {chapter}:{i + 1}-{i + len(segment)}",
            ))

    sql = """
        INSERT INTO chunks (version, chunk_type, start_scripture_id, end_scripture_id, label)
        VALUES %s
        ON CONFLICT (version, chunk_type, start_scripture_id, end_scripture_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, chunk_rows, page_size=1000)
        count = cur.rowcount
    conn.commit()
    print(f"  Segment chunks ({segment_size}-verse): {count}")


def parse_pericopes(path: str) -> list[dict]:
    """Parse PERICOPES.md into structured records."""
    pericopes = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("- "):
                continue
            line = line[2:]  # strip "- "
            parts = line.split(" - ", 1)
            if len(parts) != 2:
                continue
            ref, title = parts
            ref = ref.strip()
            title = title.strip()

            # Parse "GEN 01:1-25" or "GEN 01:1"
            tokens = ref.split()
            if len(tokens) != 2:
                continue
            book = tokens[0]
            chap_verse = tokens[1]

            if ":" not in chap_verse:
                continue
            chap_str, verse_range = chap_verse.split(":", 1)
            chapter = int(chap_str)

            if "-" in verse_range:
                start_v, end_v = verse_range.split("-", 1)
                start_verse = int(start_v)
                end_verse = int(end_v)
            else:
                start_verse = int(verse_range)
                end_verse = start_verse

            pericopes.append({
                "book": book,
                "chapter": chapter,
                "start_verse": start_verse,
                "end_verse": end_verse,
                "title": title,
            })
    return pericopes


def build_pericope_chunks(conn, version: str):
    """Create chunks from PERICOPES.md reference data."""
    pericopes_path = os.path.join(os.path.dirname(__file__), "PERICOPES.md")
    if not os.path.exists(pericopes_path):
        print("  PERICOPES.md not found, skipping pericope chunks.")
        return

    pericopes = parse_pericopes(pericopes_path)
    print(f"  Parsed {len(pericopes)} pericopes from PERICOPES.md")

    inserted = 0
    with conn.cursor() as cur:
        for p in pericopes:
            # Find the start scripture id
            cur.execute(
                """
                SELECT id FROM scripture
                WHERE version = %s AND book = %s AND chapter = %s AND verse = %s
                """,
                (version, p["book"], p["chapter"], p["start_verse"]),
            )
            start_row = cur.fetchone()

            # Find the end scripture id
            cur.execute(
                """
                SELECT id FROM scripture
                WHERE version = %s AND book = %s AND chapter = %s AND verse = %s
                """,
                (version, p["book"], p["chapter"], p["end_verse"]),
            )
            end_row = cur.fetchone()

            if not start_row or not end_row:
                continue

            cur.execute(
                """
                INSERT INTO chunks (version, chunk_type, start_scripture_id, end_scripture_id, label)
                VALUES (%s, 'pericope', %s, %s, %s)
                ON CONFLICT (version, chunk_type, start_scripture_id, end_scripture_id) DO NOTHING
                """,
                (version, start_row[0], end_row[0], p["title"]),
            )
            inserted += cur.rowcount

    conn.commit()
    print(f"  Pericope chunks: {inserted}")


def populate_content(conn, version: str):
    """For each chunk, concatenate the scripture text and insert into content."""
    sql = """
        INSERT INTO content (chunk_id, text)
        SELECT
            c.id,
            string_agg(s.text, ' ' ORDER BY s.id)
        FROM chunks c
        JOIN scripture s
            ON s.id BETWEEN c.start_scripture_id AND c.end_scripture_id
            AND s.version = c.version
        WHERE c.version = %s
          AND NOT EXISTS (
              SELECT 1 FROM content ct WHERE ct.chunk_id = c.id
          )
        GROUP BY c.id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (version,))
        count = cur.rowcount
    conn.commit()
    print(f"  Content rows created: {count}")


def main():
    parser = argparse.ArgumentParser(description="Ingest Bible CSV into PostgreSQL")
    parser.add_argument("csv_file", help="Path to Bible CSV file")
    parser.add_argument(
        "--version", default="KJV", help="Bible version identifier (default: KJV)"
    )
    parser.add_argument(
        "--database-url",
        default=DATABASE_URL,
        help="PostgreSQL connection URL (or set DATABASE_URL env var)",
    )
    parser.add_argument(
        "--segment-size",
        type=int,
        default=5,
        help="Number of verses per segment chunk (default: 5)",
    )
    args = parser.parse_args()

    if not args.database_url:
        print("Error: DATABASE_URL not set. Pass --database-url or set the env var.")
        sys.exit(1)

    print(f"Connecting to database...")
    conn = psycopg2.connect(args.database_url)

    try:
        print("Initializing schema...")
        init_schema(conn)

        print(f"Loading CSV: {args.csv_file} (version={args.version})")
        rows = load_csv(args.csv_file, args.version)
        print(f"  Parsed {len(rows)} verses from CSV")

        print("Inserting scripture...")
        insert_scripture(conn, rows)
        print(f"  Done.")

        print("Building chunks...")
        build_verse_chunks(conn, args.version)
        build_chapter_chunks(conn, args.version)
        build_segment_chunks(conn, args.version, segment_size=args.segment_size)
        build_pericope_chunks(conn, args.version)

        print("Populating content text...")
        populate_content(conn, args.version)

        print("\nIngestion complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
