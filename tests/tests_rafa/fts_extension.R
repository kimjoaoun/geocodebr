

DBI::dbExecute(con, "FORCE INSTALL fts")
DBI::dbExecute(con, "LOAD fts")

#' usando a funcao `match_bm25()` da extansao fts para fuzzy matching
#' https://duckdb.org/docs/extensions/full_text_search.html#stem-function
#'
#'
#' no exemplo abaixo, a funcao faz exact match de estado e muni
#' mas faz fuzzy match da concatenacao dos campos c(logra, numero, cep, bairro)

query_fuzzy_match <- "CREATE TEMPORARY TABLE output_caso_01 AS
  SELECT input_padrao_db.ID, AVG(filtered_cnefe_cep.lon) AS lon, AVG(filtered_cnefe_cep.lat) AS lat
  FROM input_padrao_db
  LEFT JOIN filtered_cnefe_cep
  ON input_padrao_db.estado = filtered_cnefe_cep.estado -- Still using exact match for 'estado'
  AND input_padrao_db.municipio = filtered_cnefe_cep.municipio -- Still using exact match for 'municipio'
  WHERE match_bm25(
      input_padrao_db.logradouro,
      filtered_cnefe_cep.logradouro) > 0 -- Filter for positive BM25 scores, meaning a match is found
  GROUP BY input_padrao_db.ID;"
