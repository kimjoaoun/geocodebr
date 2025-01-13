


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



###### left join com FTS -------------------------------------------

# devtools::load_all('.')
library(duckdb)
library(dplyr)
library(glue)
library(DBI)

con <-   con <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir= ":memory:"
  )


# DBI::dbExecute(con, "FORCE INSTALL fts")
DBI::dbExecute(con, "LOAD fts")


# create table
query_create_input_table <- glue::glue(
  "CREATE OR REPLACE TABLE input_db (
    id INTEGER,
    municipio VARCHAR,
    logradouro VARCHAR,
    numero INTEGER,
    cep VARCHAR
    );
INSERT INTO input_db
    VALUES (1,
            'RIO',
            'RUA ALMIRANTE TAMANDAREH',
            100,
            '70355-030'
            );")

query_create_cnefe_table <- glue::glue(
  "CREATE OR REPLACE TABLE cnefe (
    cnefe_id VARCHAR,
    municipio VARCHAR,
    logradouro VARCHAR,
    numero INTEGER,
    cep VARCHAR,
    lat NUMERIC
);
INSERT INTO cnefe
    VALUES ('a',
            'RIO',
            'RUA ALMIRANTE TAMANDARE',
            100,
            '70355-030',
            -42.1),
           ('b',
           'RIO',
           'PRACA ALMIRANTE TAMANDARE',
            150,
            '70355-031',
            -42.2
           );")

DBI::dbExecute(con, query_create_input_table)
DBI::dbExecute(con, query_create_cnefe_table)

input_db <- DBI::dbReadTable(con, 'input_db')
cnefe <- DBI::dbReadTable(con, 'cnefe')


# Create FTS index on cnefe.logradouro
query_fts_index <- glue::glue(
  "PRAGMA create_fts_index('cnefe', 'cnefe_id', 'logradouro', stemmer='portuguese'
  );"
)
DBI::dbExecute(con, query_fts_index)



DBI::dbListTables(con)

# https://duckdb.org/2021/01/25/full-text-search.html

# Perform the LEFT JOIN with match_bm25 scoring
query_left_join <- glue::glue(
  "SELECT
      input_db.logradouro AS input_logradouro,
      input_db.numero AS input_numero,
      input_db.cep AS input_cep,
      cnefe.logradouro AS cnefe_logradouro,
      cnefe.numero AS cnefe_numero,
      cnefe.cep AS cnefe_cep,
      cnefe.lat AS cnefe_lat,
     fts_main_cnefe.match_bm25(input_db.logradouro, cnefe.logradouro) AS score
    FROM
      input_db
    LEFT JOIN
      cnefe
    ON
      input_db.municipio = cnefe.municipio
  ")

# jaccard(input_db.logradouro, cnefe.logradouro) AS score
# levenshtein(input_db.logradouro, cnefe.logradouro) AS score
# fts_main_cnefe.match_bm25(input_db.logradouro, cnefe.logradouro) AS score


result <- DBI::dbGetQuery(con, query_left_join)
result

# View the result
print(result)


