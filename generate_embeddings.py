"""Generate OpenAI embeddings for content rows that don't have them yet.

Usage:
    python generate_embeddings.py [--batch-size 100]

Environment variables:
    DATABASE_URL   - PostgreSQL connection string
    OPENAI_API_KEY - OpenAI API key
"""

import argparse
import os
import sys
import time

import psycopg2
from openai import OpenAI


DATABASE_URL = os.environ.get("DATABASE_URL")
EMBEDDING_MODEL = "text-embedding-3-large"  # 3072 dimensions
MAX_TOKENS_PER_REQUEST = 8000  # stay well under the 8191 token limit per input


def get_pending_content(conn, batch_size: int) -> list[tuple[int, str]]:
    """Fetch content rows that have no embedding yet."""
    sql = """
        SELECT id, text
        FROM content
        WHERE embedding IS NULL
        ORDER BY id
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (batch_size,))
        return cur.fetchall()


def generate_embeddings(client: OpenAI, texts: list[str]) -> list[list[float]]:
    """Call OpenAI embeddings API for a batch of texts."""
    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts,
    )
    return [item.embedding for item in response.data]


def update_embeddings(conn, ids_and_vectors: list[tuple[int, list[float]]]):
    """Write embedding vectors back to the content table."""
    sql = "UPDATE content SET embedding = %s::vector WHERE id = %s"
    with conn.cursor() as cur:
        for content_id, vector in ids_and_vectors:
            cur.execute(sql, (str(vector), content_id))
    conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Generate OpenAI embeddings for Bible content"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of content rows per API call (default: 100)",
    )
    parser.add_argument(
        "--database-url",
        default=DATABASE_URL,
        help="PostgreSQL connection URL (or set DATABASE_URL env var)",
    )
    parser.add_argument(
        "--openai-api-key",
        default=os.environ.get("OPENAI_API_KEY"),
        help="OpenAI API key (or set OPENAI_API_KEY env var)",
    )
    args = parser.parse_args()

    if not args.database_url:
        print("Error: DATABASE_URL not set. Pass --database-url or set the env var.")
        sys.exit(1)

    if not args.openai_api_key:
        print("Error: OPENAI_API_KEY not set. Pass --openai-api-key or set the env var.")
        sys.exit(1)

    client = OpenAI(api_key=args.openai_api_key)

    print("Connecting to database...")
    conn = psycopg2.connect(args.database_url)

    try:
        # Count total pending
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM content WHERE embedding IS NULL")
            total_pending = cur.fetchone()[0]

        if total_pending == 0:
            print("No content rows need embeddings. Done.")
            return

        print(f"Content rows needing embeddings: {total_pending}")
        processed = 0

        while True:
            batch = get_pending_content(conn, args.batch_size)
            if not batch:
                break

            ids = [row[0] for row in batch]
            texts = [row[1] for row in batch]

            # Truncate very long texts to stay within token limits
            texts = [t[:30000] if len(t) > 30000 else t for t in texts]

            try:
                vectors = generate_embeddings(client, texts)
            except Exception as e:
                print(f"\nAPI error: {e}")
                print("Retrying in 10 seconds...")
                time.sleep(10)
                try:
                    vectors = generate_embeddings(client, texts)
                except Exception as e2:
                    print(f"Retry failed: {e2}")
                    print(f"Stopping. {processed}/{total_pending} processed so far.")
                    break

            pairs = list(zip(ids, vectors))
            update_embeddings(conn, pairs)

            processed += len(batch)
            pct = (processed / total_pending) * 100
            print(f"  Embedded {processed}/{total_pending} ({pct:.1f}%)")

            # Rate limit courtesy pause
            time.sleep(0.1)

        print(f"\nEmbedding complete. {processed} rows processed.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
