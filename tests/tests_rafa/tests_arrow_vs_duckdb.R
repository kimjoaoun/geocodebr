#' TODO
#' instalar extensoes do duckdb
#' - allocate memory
#' - spatial
#' - arrow
#' - jemalloc (memory allocation) / not available on windows
#'
#' #' (non-deterministic search)
#' - fts - Adds support for Full-Text Search Indexes / "https://medium.com/@havus.it/enhancing-database-search-full-text-search-fts-in-mysql-1bb548f4b9ba"
#'
#'
#'
#' adicionar dados de POI da Meta /overture
#' adicionar dados de enderecos Meta /overture

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
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
names(input_df)[1] <- 'id'
input_df$id <-  1:nrow(input_df)


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

rafa <- function(){
  df_duck_rafa <- geocodebr:::geocode_rafa(
    input_table = input_df,
    logradouro = "nm_logradouro",
    numero = "Numero",
    cep = "Cep",
    bairro = "Bairro",
    municipio = "nm_municipio",
    estado = "nm_uf",
    output_simple = F,
    n_cores=7,
    progress = T
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

microbenchmark::microbenchmark(dani = dani(),
                               rafa = rafa(),
                               times = 10
)

# expr       min        lq      mean    median        uq      max neval
# dani 16.006761 16.859789 18.569111 17.691887 20.367627 22.86581    10
# rafa  7.586888  8.178381  9.156078  8.865305  9.168628 13.42151    10


# precision ------------------------------------------------------------------

input_df <- input_table <- data.frame(
  id=666,
  nm_logradouro = 'SQS 308 Bloco C',
  Numero = 204,
  Complemento = 'Bloco C',
  Cep = 70355030,
  Bairro = 'Asa sul',
  nm_municipio = 'Brasilia',
  nm_uf = 'DF'
)

cnefe_cep <- arrow::open_dataset( geocodebr::get_cache_dir())
df <- filter(cnefe_cep, cep =='70355-030') |> collect()




# LIKE operator ------------------------------------------------------------------
input_df$Complemento <- NULL
input_df$code_muni <- NULL

input_df_like <- data.frame(
  id=666,
  nm_logradouro = 'DESEMBARGADOR HUGO SIMAS',
  Numero = 1948,
  Cep = '80520-250',
  Bairro = 'BOM RETIRO',
  nm_municipio = 'CURITIBA',
  nm_uf = 'PARANA'
)

input_df_like <- rbind(input_df_like, input_df)

fields <- geocodebr::setup_address_fields(
  logradouro = 'nm_logradouro',
  numero = 'Numero',
  cep = 'Cep',
  bairro = 'Bairro',
  municipio = 'nm_municipio',
  estado = 'nm_uf'
)

lik <- geocodebr:::geocode_like(
  addresses_table = input_df_like,
  address_fields = fields,
  n_cores = 1, # 7
  progress = F
)

head(lik)

lik <- geocodebr:::geocode_rafa_like(
  input_table = input_df_like,
  logradouro = "nm_logradouro",
  numero = "Numero",
  cep = "Cep",
  bairro = "Bairro",
  municipio = "nm_municipio",
  estado = "nm_uf",
  output_simple = F,
  n_cores=7,
  progress = T
)

geocodebr::get_cache_dir() |>
  geocodebr:::arrow_open_dataset()  |>
  filter(estado=="PR") |>
  filter(municipio == "CURITIBA") |>
  dplyr::compute() |>
  filter(logradouro_sem_numero %like% "DESEMBARGADOR HUGO SIMAS") |>
  dplyr::collect()



################## calculate precision as the area in m2
range_lon <- max(df$lon) - min(df$lon)
range_lat <- max(df$lat) - min(df$lat)

lon_meters <- 111320 * range_lon * cos(mean(df$lat))
lat_meters <- 111320 * range_lat

area = pi * lon_meters * lat_meters
area
##################
range_lon <- sd(df$lon) *2
range_lat <- sd(df$lat) *2



query_aggregate_and_match <- sprintf(
  "CREATE TABLE %s AS
    WITH pre_aggregated_cnefe AS (
      SELECT %s AVG(lon) AS lon, AVG(lat) AS lat,
      2 * STDDEV_SAMP(lon) as range_lon, 2 * STDDEV_SAMP(lat) as range_lat
      FROM %s
      GROUP BY %s
    )
    SELECT %s.ID, pre_aggregated_cnefe.lon, pre_aggregated_cnefe.lat,
    pre_aggregated_cnefe.range_lon, pre_aggregated_cnefe.range_lat
    FROM %s AS %s
    LEFT JOIN pre_aggregated_cnefe
    ON %s
    WHERE pre_aggregated_cnefe.lon IS NOT NULL;",
  output_tb,     # new table
  cols_select,   # select
  y,             # from
  cols_group,    # group
  x,             # select
  x, x,          # from
  join_condition # on
)



