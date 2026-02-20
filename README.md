# Bible RAG
database w/ vector embeddings for bibles

## Schema

scripture (
  id,
  version,
  book,
  chapter,
  verse,
  text
)

chunks (
  id,
  startScriptureId,
  startScriptureId,
  endChapter
)

content (
  id,
  chunkId,
  text,
  vector
)


## Process
* upload your bible text toxt the scripture table
* initial chunks
  * verses
  * chapters
  * 5-verse segments (in-order)
  * pericopes (natural breaks in the text, often titled in some versions)
* scripture segments get chunked and embedded
