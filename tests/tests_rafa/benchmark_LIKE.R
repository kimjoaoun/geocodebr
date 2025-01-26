#' rua ipe roxo em sp



#' TO DO
#'
#' a) estrateia de montar output: com empty db ou tabelas separadas por case_match ?
#' b) manter columna matched_address
#' c) dani arrow
#'
#' instalar extensoes do duckdb
#' - spatial - acho q nao vale a pena por agora
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
#'   - (next) join probabilistico com fts_main_documents.match_bm25
#'   - (next) calcular nivel de erro na agregacao do cnefe
#'   - optimize disk and parallel operations in duckdb
#'   - casos de rodovias
#'   - interpolar numeros separando impares e pares
#'   - CASES NOT FOUND ? AND THEIR EFFECT ON THE PROGRESS BAR



#' take-away
#' 1) incluir LIKE no campo d elogradouro melhor MUITO performance, encontrando
#' muito mais casos em cases de match 1, 2, 3 e 4
#' o melhor mesmo seria usar fts
#' 2) isso tem pequeno efeito de diminuir performance do dani, e 0 efeito no rafa
#'
#' 3) no rafa aida tem um residudo de que alguns casos em que as coordenadas nao
#' foram agregadas, entao tem alguns 'id's que se repetem no output
#'  - a razao eh pq a agregacao sai diferente para logradouros diferentes mas
#'  com o mesmo padrao LIKE. Ex. "RUA AVELINO CAVALCANTE" e "TRAVESSA AVELINO CAVALCANTE"
#'
#'  exemplos
#' id == 1637 caso de diferentes ruas no mesmo condominio
#'            "RUA DOIS VILA RICA" e "RUA XXVI QUADRA E VILA RICA"
#' id == 1339 (esse se resolve pq sao bairros diferentes)


devtools::load_all('.')
library(dplyr)
# library(geocodebr)
# library(enderecobr)
# library(data.table)
# library(arrow)
# library(duckdb)


# open input data
data_path <- system.file("extdata/large_sample.parquet", package = "geocodebr")
input_df <- arrow::read_parquet(data_path)



# enderecos = input_df
# n_cores = 7
# ncores <- 7
# verboso = T
# cache = TRUE
# resultado_completo = T
# resultado_sf = F
# campos_endereco <- geocodebr::listar_campos(
#   logradouro = 'logradouro',
#   numero = 'numero',
#   cep = 'cep',
#   localidade = 'bairro',
#   municipio = 'municipio',
#   estado = 'uf')



# benchmark different approaches ------------------------------------------------------------------
ncores <- 7



campos <- geocodebr::listar_campos(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  localidade = 'bairro',
  municipio = 'municipio',
  estado = 'uf'
)

rafaF <- function(){ message('rafa F')
  df_rafaF <- geocodebr::geocode(
    enderecos = input_df,
    campos_endereco = campos,
    n_cores = ncores,
    resultado_completo = T,
    verboso = F,
    resultado_sf = F
  )
}


rafaT <- function(){ message('rafa F')
  df_rafaT <- geocodebr::geocode(
    enderecos = input_df,
    campos_endereco = campos,
    n_cores = ncores,
    resultado_completo = T,
    verboso = T
  )
}






mb <- microbenchmark::microbenchmark(
  rafa_drop = rafaF(),
  dani_drop = dani_arrowF(),
  rafa_keep = rafaT(),
  dani_keep = dani_arrowT(),
  times  = 1
)
mb

library(profvis)
profvis({
  rafaT()
})



# 20K FRESH
# Unit: seconds
#         expr      min       lq     mean   median       uq      max neval
#    rafa_drop 10.10379 10.25397 10.35726 10.39609 10.41682 10.61564     5
# rafa_drop_db 10.33163 10.41716 10.53599 10.57100 10.67138 10.68880     5
#    rafa_keep 11.12560 11.17642 11.40483 11.30231 11.56706 11.85277     5
# rafa_keep_db 10.99093 11.18329 11.26758 11.18585 11.40816 11.56966     5
#      dani_df 10.99375 11.04987 11.47305 11.16092 11.43530 12.72541     5





bm <- bench::mark(
  rafa_drop = rafaF(),
  rafa_drop_db = rafaF_db(),
  rafa_keep = rafaT(),
  rafa_keep_db = rafaT_db(),
  dani_df = dani_arrow(),
  check = F,
  iterations  = 5
)
bm

# 20 K
#     expression      min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time
#     <bch:expr>   <bch:> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>     <list>
#   1 rafa_drop     10.4s  10.5s    0.0952      53MB    0.171     5     9      52.5s <NULL> <Rprofmem> <bench_tm>
#   2 rafa_drop_db  10.6s  10.7s    0.0935    27.5MB    0.168     5     9     53.46s <NULL> <Rprofmem> <bench_tm>
#   3 rafa_keep     11.5s  11.7s    0.0859    27.7MB    0.155     5     9     58.23s <NULL> <Rprofmem> <bench_tm>
#   4 rafa_keep_db  11.3s  11.6s    0.0867    27.6MB    0.139     5     8     57.69s <NULL> <Rprofmem> <bench_tm>
#   5 dani_df       11.5s  11.9s    0.0823    24.9MB    0.148     5     9      1.01m <NULL> <Rprofmem> <bench_tm>





devtools::load_all('.')
library(tictoc)
library(dplyr)
library(data.table)
library(ipeadatalake)
library(mapview)
library(sfheaders)
library(sf)
options(scipen = 999)
mapview::mapviewOptions(platform = 'leafgl')
set.seed(42)





geocodebr::get_cache_dir() |>
  geocodebr:::arrow_open_dataset()  |>
  filter(estado=="PR") |>
  filter(municipio == "CURITIBA") |>
  dplyr::compute() |>
  filter(logradouro_sem_numero %like% "DESEMBARGADOR HUGO SIMAS") |>
  dplyr::collect()


cnf <- ipeadatalake::read_cnefe(year = 2022) |>
  #  filter(code_state=="41") |>
  dplyr::filter(code_muni == 4106902) |>
  dplyr::collect()


d <- cnf |>
  filter(nom_seglogr %like% "HUGO SIMAS")

'DESEMBARGADOR'






# small sample data ------------------------------------------------------------------


# open input data
data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
input_df <- read.csv(data_path, encoding = 'Latin-1')



campos <- geocodebr::listar_campos(
  logradouro = 'nm_logradouro',
  numero = 'Numero',
  cep = 'Cep',
  localidade = 'Bairro',
  municipio = 'nm_municipio',
  estado = 'nm_uf'
)
# enderecos = input_df
# campos_endereco = campos
# n_cores = 7
# verboso = T
# cache=T
# resultado_completo=T

rafaF <- function(){ message('rafa F')
  df_rafaF <- geocodebr::geocode(
    enderecos = input_df,
    campos_endereco = campos,
    n_cores = 7,
    resultado_completo = T,
    verboso = T
  )
}

identical(df_rafaF$id, input_df$id)


table(df_rafa_loop$precision) / nrow(df_rafa_loop)*100


unique(df_rafa$match_type) |> length()

table(df_rafa$match_type)


  # en01: logradouro, numero, cep e bairro
  # en02: logradouro, numero e cep
  # en03: logradouro, numero e bairro
  # en04: logradouro e numero
      # pn01: logradouro, numero, cep e bairro
      # pn02: logradouro, numero e cep
      # pn03: logradouro, numero e bairro
      # pn04: logradouro e numero
  # ei01: logradouro, numero, cep e bairro
  # ei02: logradouro, numero e cep
  # ei03: logradouro, numero e bairro
  # ei04: logradouro e numero
        # pi01: logradouro, numero, cep e bairro
        # pi02: logradouro, numero e cep
        # pi03: logradouro, numero e bairro
        # pi04: logradouro e numero
  # er01: logradouro, cep e bairro
  # er02: logradouro e cep
  # er03: logradouro e bairro
  # er04: logradouro
      # pr01: logradouro, cep e bairro
      # pr02: logradouro e cep
      # pr03: logradouro e bairro
      # pr04: logradouro
  # ec01: municipio, cep, localidade
  # ec02: municipio, cep
  # eb01: municipio, localidade
  # em01: municipio
