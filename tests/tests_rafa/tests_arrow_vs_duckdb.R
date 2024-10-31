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


# library(geocodebr)
library(tictoc)
# library(enderecobr)
# library(data.table)
# library(dplyr)
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
# showProgress = TRUE
# output_simple = TRUE
# ncores = NULL
# cache = TRUE





tictoc::tic()
df_duck3 <- geocode(
  input_table = input_df,
  logradouro = "nm_logradouro",
  numero = "Numero",
  complemento = "Complemento",
  cep = "Cep",
  bairro = "Bairro",
  municipio = "nm_municipio",
  estado = "nm_uf",
  output_simple = F,
  ncores=NULL,
  showProgress = T
)
tictoc::toc()
#> 28: 4 - 5  (in memory)
#> 28: 4 - 5  (disk)
#>
#> 900K: 37.11 - 44.56  (in memory)
#> 900K: 39.34          (disk) match_case
#> 900K: 16             (disk) match_aggregated_cases
#>
#> 7.3 milhoes: 113.01 (in memory)
#> 7.3 milhoes: 101.31 (disk) match_case
#> 7.3 milhoes: 57.36 (disk) match_aggregated_cases
