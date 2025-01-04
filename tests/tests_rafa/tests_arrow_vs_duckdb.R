#' all possible cases
#'
#' 01:04 logradouro_deterministico & numero exato
#' 05:08 logradouro_deterministico & numero interpolado
#' 09:12 logradouro_deterministico & numero 'S/N'
#'
#' 13:16 logradouro_probabilistico & numero exato
#' 17:20 logradouro_probabilistico & numero interpolado
#' 21:24 logradouro_probabilistico & numero 'S/N'
#'
#'  25 municipio, cep, localidade
#'  26 municipio, cep
#'  27 municipio, localidade
#'  28 municipio
#'
#'
#' TO DO
#'
#' instalar extensoes do duckdb
#' - spatial - acho q nao vale a pena por agora
#' - arrow - faria diferenca?
#'
#' #' (non-deterministic search)
#' - fts - Adds support for Full-Text Search Indexes / "https://medium.com/@havus.it/enhancing-database-search-full-text-search-fts-in-mysql-1bb548f4b9ba"
#'
#'
#'
#' adicionar dados de POI da Meta /overture
#' adicionar dados de enderecos Meta /overture
#'   # NEXT STEPS
#'   - (ok) interpolar numeros na mesma rua
#'   - join probabilistico com fts_main_documents.match_bm25
#'   - optimize disk and parallel operations in duckdb
#'   - exceptional cases (no info on municipio input)
#'   - calcular nivel de erro



## CASE 999 --------------------------------------------------------------------
# TO DO
# WHAT SHOULD BE DONE FOR CASES NOT FOUND ?
# AND THEIR EFFECT ON THE PROGRESS BAR





devtools::load_all('.')
library(tictoc)
library(dplyr)
# library(geocodebr)
# library(enderecobr)
# library(data.table)
# library(arrow)
# library(duckdb)


# open input data
data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
input_df <- read.csv(data_path)



# input_table = input_df
# logradouro = "nm_logradouro"
# numero = "Numero"
# complemento = "Complemento"
# cep = "Cep"
# bairro = "Bairro"
# municipio = "nm_municipio"
# estado = "nm_uf"
# progress = TRUE
# output_simple = TRUE
# n_cores = 1
# cache = TRUE

# benchmark different approaches ------------------------------------------------------------------

rafa_loop <- function(){
  fields <- geocodebr::setup_address_fields(
    logradouro = 'nm_logradouro',
    numero = 'Numero',
    cep = 'Cep',
    bairro = 'Bairro',
    municipio = 'nm_municipio',
    estado = 'nm_uf'
  )

  df_rafa_loop <- geocodebr:::geocode_rafa(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = 7,
    progress = T,
    cache=T
  )
}


rafa_loc <- function(){
  fields <- geocodebr::setup_address_fields(
    logradouro = 'nm_logradouro',
    numero = 'Numero',
    cep = 'Cep',
    bairro = 'Bairro',
    municipio = 'nm_municipio',
    estado = 'nm_uf'
  )

  df_rafa_local <- geocodebr:::geocode_rafa_local(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = 7,
    progress = T,
    cache=T
  )
}


rafa_loc2 <- function(){
  fields <- geocodebr::setup_address_fields(
    logradouro = 'nm_logradouro',
    numero = 'Numero',
    cep = 'Cep',
    bairro = 'Bairro',
    municipio = 'nm_municipio',
    estado = 'nm_uf'
  )


  df_rafa_local <- geocodebr:::geocode_rafa_local2(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = 7,
    progress = T,
    cache=T
  )
}


dani <- function(){
  fields <- geocodebr::setup_address_fields(
    logradouro = 'nm_logradouro',
    numero = 'Numero',
    cep = 'Cep',
    bairro = 'Bairro',
    municipio = 'nm_municipio',
    estado = 'nm_uf'
  )

  df_duck_dani <- geocodebr:::geocode(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = 7,
    progress = T
  )
}

microbenchmark::microbenchmark(
  dani = dani(),
  rafa_loop = rafa_loop(),
  rafa_loc = rafa_loc(),
  rafa_loc2 = rafa_loc2(),
  times = 1
  )

# expr           min       lq     mean   median       uq      max neval
#      dani 8.387754 8.422850 8.795608 8.677266 8.897707 9.785240    10
# rafa_loop 7.052954 7.208139 7.358788 7.340292 7.501446 7.778086    10
#      rafa 6.907185 6.991580 7.215487 7.036608 7.380152 8.095192    10


# rafa loop: 12,4995.6 per second com 917K cases

# First open a connection to the database


####### persistent db ----------------------------------------
# tabelas pre-agregadas ok pq ficam menores, mas sao varias
# mas ainda precisaria do dado completo para fazer interpolacao de coordenadas
# tabelas pre-agregadas salva certo tempo, mas teria q narrow scope para os merges
# entao soh testando pra saber se compensa

devtools::load_all('.')
library(dplyr)

con <- duckdb::dbConnect(duckdb::duckdb(), dbdir= 'geo.duckdb' ) # db_path ":memory:"
cnefe <- arrow::open_dataset(get_cache_dir())
nrow(cnefe)


DBI::dbWriteTableArrow(con, "cnefe", cnefe,
                     temporary = FALSE, overwrite = TRUE)

data <- data.frame(id = 1:5, value = letters[1:5])
data
DBI::dbWriteTable(con, "example_table", data)


# nao funciona
# duckdb::duckdb_register_arrow(con, "cnefe", cnefe)

# ?
# DBI::dbCreateTableArrow(con, "cnefe", cnefe,
#                      temporary = FALSE, overwrite = TRUE)

DBI::dbDisconnect(con, shutdown = TRUE)


# new session
con2 <- duckdb::dbConnect(duckdb::duckdb(), dbdir = 'geo.duckdb')
DBI::dbListTables(con2)

DBI::dbGetQuery(con, "SELECT * FROM cnefe LIMIT 5;")
DBI::dbGetQueryArrow(con, "SELECT * FROM cnefe LIMIT 5;")

a <- DBI::dbSendQueryArrow(con, "SELECT * FROM cnefe LIMIT 5;")

DBI::dbGetQuery(con,
'SELECT COUNT(*) AS row_count
 FROM cnefe;'
)


microbenchmark::microbenchmark(
  # dani = dani(),
  rafa_loop = rafa_loop(),
  rafa_loc = rafa_loc(),
  times = 5
)

# large sample
#      expr      min       lq     mean   median       uq      max neval
# rafa_loop 40.81682 42.97777 44.17992 44.41040 45.27371 47.42092     5
#  rafa_loc 17.54525 17.97168 18.54324 18.33696 19.21377 19.64852     5

# small sample
#      expr       min        lq      mean    median        uq       max neval
# rafa_loop 10.080199 10.963517 10.996075 11.193744 11.246779 11.496137     5
#  rafa_loc  6.980258  6.982212  7.071433  6.997702  7.166068  7.230925     5



