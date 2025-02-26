


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
      input_db.id,
      input_db.logradouro AS input_logradouro,
      cnefe.logradouro AS cnefe_logradouro,
      cnefe.lat AS cnefe_lat,
     fts_main_cnefe.match_bm25(input_logradouro, cnefe_logradouro) AS score
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






# solucao sem fts -------------------------------------

#' https://duckdb.org/docs/sql/functions/char.html
#' damerau_levenshtein
#' jaccard
#' jaro_similarity
#' jaro_winkler_similarity
#'

# threshold
query_join <- glue::glue(
  "WITH similarity_scores AS (
     SELECT
       i.id AS input_id,
       c.cnefe_id,
       i.municipio AS input_municipio,
       c.municipio AS cnefe_municipio,
       i.logradouro AS input_logradouro,
       c.logradouro AS cnefe_logradouro,
       jaro_winkler_similarity(i.logradouro, c.logradouro) AS similarity
     FROM input_db i
     JOIN cnefe c
     ON i.municipio = c.municipio
   )
   SELECT
     input_id,
     cnefe_id,
     input_municipio,
     cnefe_municipio,
     input_logradouro,
     cnefe_logradouro,
     similarity
   FROM similarity_scores
   WHERE similarity > 0.8 -- Adjust the threshold as needed
   ORDER BY similarity DESC;"
)

# return max similariy
query_max_similarity <- glue::glue(
  "WITH similarity_scores AS (
     SELECT
       i.id AS input_id,
       c.cnefe_id,
       i.municipio AS input_municipio,
       c.municipio AS cnefe_municipio,
       i.logradouro AS input_logradouro,
       c.logradouro AS cnefe_logradouro,
       jaro_winkler_similarity(i.logradouro, c.logradouro) AS similarity,
       RANK() OVER (PARTITION BY i.id ORDER BY jaro_winkler_similarity(i.logradouro, c.logradouro) DESC) AS rank
     FROM input_db i
     JOIN cnefe c
     ON i.municipio = c.municipio
   )
   SELECT
     input_id,
     cnefe_id,
     input_municipio,
     cnefe_municipio,
     input_logradouro,
     cnefe_logradouro,
     similarity,
     rank
   FROM similarity_scores
   WHERE similarity > 0.8 -- Adjust the threshold as needed
   AND rank = 1 -- Only keep the best match per input;"
)



# Execute the query
DBI::dbGetQuery(con, query_join)
DBI::dbGetQuery(con, query_max_similarity)

w1 <-     'RUA PRESIDENTE JOAO GOULART'
w2 <- 'RUA PADRE ITALO COELHO'


w1 <- 'RUA JOAO HILARIO DA SILVA'
w2 <- 'RUA CARLOS MOTTA DA SILVA'


w1 <- 'RUA UM'
w2 <- 'RUA DOIS'


# q <- glue::glue("SELECT jaro_winkler_similarity('{w1}', '{w2}');")
q <- glue::glue("SELECT jaccard('{w1}', '{w2}') AS similarity;")
DBI::dbGetQuery(con, q)



#' jaccard
#' jaro_similarity
#' jaro_winkler_similarity
#' DICE https://stackoverflow.com/questions/51610033/getting-similar-strings-in-pl-sql-with-a-good-performance
#'    MDS usa valores iguais ou superiores a 0,6








library(dplyr)
library(sf)
library(sfheaders)
library(arrow)
library(mapview)

tudo <- geocodebr::listar_dados_cache()
tudo <- tudo[7]

cnefe <- arrow::open_dataset( tudo ) |>
  dplyr::filter(estado == 'RJ') |>
  dplyr::filter(municipio == "RIO DE JANEIRO") |>
  dplyr::filter(logradouro == "AVENIDA MINISTRO IVAN LINS") |>
  dplyr::collect()


check_approx <- function(numero_input, expp){

input <- data.frame(
  id = 1,
  logradouro = 'AVENIDA MINISTRO IVAN LINS',
  numero_input = numero_input,
  localidade = "BARRA DA TIJUCA",
  cep = '22620-110'
  )

df <- left_join(input, cnefe, by = c('logradouro', 'localidade', 'cep'))


output <- df |>
  group_by(id) |>
  summarize(
    numero_input = first(numero_input),
    lat = sum((1/abs(numero_input - numero)^expp * lat)) / sum(1/abs(numero_input - numero)^expp),
    lon = sum((1/abs(numero_input - numero)^expp * lon)) / sum(1/abs(numero_input - numero)^expp)
    )

sf_cnefe <- sfheaders::sf_point(
  obj = cnefe,
  x = 'lon',
  y = 'lat',
  keep = TRUE
)

sf_output <- sfheaders::sf_point(
  obj = output,
  x = 'lon',
  y = 'lat',
  keep = TRUE
)


sf::st_crs(sf_cnefe) <- 4674
sf::st_crs(sf_output) <- 4674

mapp <- mapview::mapview(sf_cnefe, zcol='numero') +
  mapview(sf_output, col.regions = "red")

return(mapp)
}


approx_do_rogerio = function(numero_input, dados_rua){

  f_lat = with(dados_rua, approxfun(x = numero, y = lat, rule = 2, method = "linear"))
  f_lon = with(dados_rua, approxfun(x = numero, y = lon, rule = 2, method = "linear"))

  output = data.frame(lat = f_lat(numero_input),
                      lon = f_lon(numero_input))

  sf_output_rogerio <- sfheaders::sf_point(
    obj = output,
    x = 'lon',
    y = 'lat',
    keep = TRUE
  )

  sf::st_crs(sf_output_rogerio) <- 4674

  mapview(sf_output_rogerio, col.regions = "orange")
}

check_approx(numero_input = 6, expp = 1)
check_approx(numero_input = 6, expp = 3)

check_approx(numero_input = 760, expp = 1)
check_approx(numero_input = 760, expp = 10)

check_approx(numero_input = 3423412341234213, expp = 40)




approx_do_rogerio(numero_input = 100000, dados_rua = cnefe)


# aprox SQL -------------------------------


tudo <- geocodebr::listar_dados_cache()
tudo <- tudo[7]

cnefe <- arrow::open_dataset( tudo ) |>
  dplyr::filter(estado == 'RJ') |>
  dplyr::filter(municipio == "RIO DE JANEIRO") |>
  dplyr::filter(logradouro == "AVENIDA MINISTRO IVAN LINS") |>
  dplyr::collect()


input <- data.frame(
  id = 1,
  logradouro = 'AVENIDA MINISTRO IVAN LINS',
  numero_input = 5,
  localidade = "BARRA DA TIJUCA",
  cep = '22620-110'
)


df <- left_join(input, cnefe, by = c('logradouro', 'localidade', 'cep'))

con <- geocodebr:::create_geocodebr_db()


duckdb::dbWriteTable(con, "data_frame_ruas", df, overwrite = TRUE, temporary = TRUE)


approximacao_sql <- function(numero_input, con) {
  query <- sprintf("
    WITH bounds AS (
      SELECT
        (SELECT MIN(numero) FROM data_frame_ruas) AS min_num,
        (SELECT MAX(numero) FROM data_frame_ruas) AS max_num,
        (SELECT MAX(numero) FROM data_frame_ruas WHERE numero <= %f) AS lower_num,
        (SELECT MIN(numero) FROM data_frame_ruas WHERE numero >= %f) AS upper_num
    ),
    values_cte AS (
      SELECT
        (SELECT lat FROM data_frame_ruas ORDER BY numero ASC LIMIT 1) AS min_lat,
        (SELECT lon FROM data_frame_ruas ORDER BY numero ASC LIMIT 1) AS min_lon,
        (SELECT lat FROM data_frame_ruas ORDER BY numero DESC LIMIT 1) AS max_lat,
        (SELECT lon FROM data_frame_ruas ORDER BY numero DESC LIMIT 1) AS max_lon,
        (SELECT lat FROM data_frame_ruas WHERE numero = (SELECT MAX(numero) FROM data_frame_ruas WHERE numero <= %f)) AS lower_lat,
        (SELECT lon FROM data_frame_ruas WHERE numero = (SELECT MAX(numero) FROM data_frame_ruas WHERE numero <= %f)) AS lower_lon,
        (SELECT lat FROM data_frame_ruas WHERE numero = (SELECT MIN(numero) FROM data_frame_ruas WHERE numero >= %f)) AS upper_lat,
        (SELECT lon FROM data_frame_ruas WHERE numero = (SELECT MIN(numero) FROM data_frame_ruas WHERE numero >= %f)) AS upper_lon
    ),
    interp AS (
      SELECT
        b.min_num, b.max_num, b.lower_num, b.upper_num,
        v.min_lat, v.min_lon, v.max_lat, v.max_lon,
        v.lower_lat, v.lower_lon, v.upper_lat, v.upper_lon
      FROM bounds b, values_cte v
    )
    SELECT
      CASE
        WHEN %f <= min_num THEN min_lat
        WHEN %f >= max_num THEN max_lat
        WHEN lower_num = upper_num THEN lower_lat
        ELSE lower_lat + ((%f - lower_num) * (upper_lat - lower_lat)) / (upper_num - lower_num)
      END AS lat,
      CASE
        WHEN %f <= min_num THEN min_lon
        WHEN %f >= max_num THEN max_lon
        WHEN lower_num = upper_num THEN lower_lon
        ELSE lower_lon + ((%f - lower_num) * (upper_lon - lower_lon)) / (upper_num - lower_num)
      END AS lon
    FROM interp
    LIMIT 1;
  ",
                   numero_input, numero_input,           # For lower_num and upper_num in bounds
                   numero_input, numero_input, numero_input, numero_input,  # For lower_lat, lower_lon, upper_lat, upper_lon in values_cte
                   numero_input, numero_input, numero_input,  # For lat: conditions and numerator in interpolation
                   numero_input, numero_input, numero_input   # For lon: conditions and numerator in interpolation
  )

  res <- DBI::dbGetQuery(con, query)
  return(as.matrix(res))
}


numm <- 1

bench::system_time(
  a <- approximacao_sql(numm, con)
)

bench::system_time(
  a <- approximacao_sql_optimized(numm, con)
)

a <- sfheaders::sf_point(
  obj = a,
  x = 'lon',
  y = 'lat',
  keep = TRUE
)

sf::st_crs(a) <- 4674



roger <- approx_do_rogerio(numero_input = numm, dados_rua = cnefe)


roger + a



approximacao_sql_optimized <- function(numero_input, con) {
  query <- sprintf("
    WITH bounds AS (
      SELECT MIN(numero) AS min_num, MAX(numero) AS max_num
      FROM data_frame_ruas
    ),
    lower_row AS (
      SELECT numero, lat, lon
      FROM data_frame_ruas
      WHERE numero = (
        CASE
          WHEN %f <= (SELECT min_num FROM bounds)
            THEN (SELECT min_num FROM bounds)
          ELSE (SELECT MAX(numero) FROM data_frame_ruas WHERE numero <= %f)
        END
      )
      LIMIT 1
    ),
    upper_row AS (
      SELECT numero, lat, lon
      FROM data_frame_ruas
      WHERE numero = (
        CASE
          WHEN %f >= (SELECT max_num FROM bounds)
            THEN (SELECT max_num FROM bounds)
          ELSE (SELECT MIN(numero) FROM data_frame_ruas WHERE numero >= %f)
        END
      )
      LIMIT 1
    )
    SELECT
      CASE
        WHEN lr.numero = ur.numero THEN lr.lat
        ELSE lr.lat + ((%f - lr.numero) * (ur.lat - lr.lat)) / (ur.numero - lr.numero)
      END AS lat,
      CASE
        WHEN lr.numero = ur.numero THEN lr.lon
        ELSE lr.lon + ((%f - lr.numero) * (ur.lon - lr.lon)) / (ur.numero - lr.numero)
      END AS lon
    FROM lower_row lr
    CROSS JOIN upper_row ur;
  ",
                   numero_input, numero_input,    #/* for lower_row: condition and else */
                   numero_input, numero_input,  #/* for upper_row: condition and else */
                   numero_input,                #/* for lat interpolation */
                   numero_input                 #/* for lon interpolation */
  )

  res <- DBI::dbGetQuery(con, query)
  return(as.matrix(res))
}

