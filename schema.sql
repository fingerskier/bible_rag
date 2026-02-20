CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS scripture (
    id SERIAL PRIMARY KEY,
    version VARCHAR(32) NOT NULL,
    book VARCHAR(8) NOT NULL,
    chapter INTEGER NOT NULL,
    verse INTEGER NOT NULL,
    text TEXT NOT NULL,
    UNIQUE (version, book, chapter, verse)
);

CREATE TABLE IF NOT EXISTS chunks (
    id SERIAL PRIMARY KEY,
    version VARCHAR(32) NOT NULL,
    chunk_type VARCHAR(16) NOT NULL,  -- verse, chapter, segment, pericope
    start_scripture_id INTEGER NOT NULL REFERENCES scripture(id),
    end_scripture_id INTEGER NOT NULL REFERENCES scripture(id),
    label TEXT,  -- optional description, e.g. pericope title
    UNIQUE (version, chunk_type, start_scripture_id, end_scripture_id)
);

CREATE TABLE IF NOT EXISTS content (
    id SERIAL PRIMARY KEY,
    chunk_id INTEGER NOT NULL REFERENCES chunks(id),
    text TEXT NOT NULL,
    embedding vector(3072)  -- text-embedding-3-large dimensions
);

CREATE INDEX IF NOT EXISTS idx_scripture_version_book ON scripture(version, book, chapter);
CREATE INDEX IF NOT EXISTS idx_chunks_type ON chunks(chunk_type);
CREATE INDEX IF NOT EXISTS idx_content_embedding ON content USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
