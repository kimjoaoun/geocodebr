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
data_path <- system.file("extdata/sample_1.csv", package = "geocodebr")
input_df <- read.csv(data_path)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df$ID <-  1:nrow(input_df)



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
# ncores = NULL
# cache = TRUE

tictoc::tic()
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
tictoc::toc()
# 900K: 13 secs


tictoc::tic()
df_duck_rafa <- geocodebr:::geocode_rafa(
  input_table = input_df,
  logradouro = "nm_logradouro",
  numero = "Numero",
  cep = "Cep",
  bairro = "Bairro",
  municipio = "nm_municipio",
  estado = "nm_uf",
  output_simple = F,
  ncores=7,
  progress = T
)
tictoc::toc()
# 900K: 19.83 secs






input_df <- input_table <- data.frame(
  ID=666,
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



