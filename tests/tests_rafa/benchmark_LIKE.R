
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


# input_table = input_df
# logradouro = "logradouro"
# numero = "numero"
# cep = "cep"
# bairro = "bairro"
# municipio = "municipio"
# estado = "uf"
# progress = TRUE
# output_simple = FALSE
# n_cores = 1
# cache = TRUE


# benchmark different approaches ------------------------------------------------------------------
ncores <- 7


# round(table(df_duck_rafa$match_type)/nrow(df_duck_rafa)*100 ,2)
#     1     2     3     4     44      9    10    11    12
# 24.41  9.78  8.80  6.17  13.32  21.46 12.89  2.53  0.65
#
# round(table(df_duck_rafa1$match_type)/nrow(df_duck_rafa1)*100 ,2)
#     1     2     3     4     5     6     7     8     9    10    11    12
# 24.41  9.78  8.80  6.17  6.18  3.03  1.98  2.13 21.46 12.89  2.53  0.65

# estranho, deveria ter caso 08 que nao entra em 44 pq nao tem numero no input
# checar se isso ocore


# rafa_like <- function(){
#   df_duck_rafa2 <- geocodebr:::geocode_rafa_like(
#     input_table = input_df,
#     logradouro = "logradouro",
#     numero = "numero",
#     cep = "cep",
#     bairro = "bairro",
#     municipio = "municipio",
#     estado = "uf",
#     output_simple = F,
#     n_cores=ncores,
#     progress = F
#   )
# }



rafa_loop <- function(){
  fields <- geocodebr::setup_address_fields(
    logradouro = 'logradouro',
    numero = 'numero',
    cep = 'cep',
    bairro = 'bairro',
    municipio = 'municipio',
    estado = 'uf'
  )

  df_rafa <- geocodebr:::geocode_rafa(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = ncores,
    progress = F
  )
}


rafa_loc <- function(){
  fields <- geocodebr::setup_address_fields(
    logradouro = 'logradouro',
    numero = 'numero',
    cep = 'cep',
    bairro = 'bairro',
    municipio = 'municipio',
    estado = 'uf'
  )

  df_rafa_local <- geocodebr:::geocode_rafa_local(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = ncores,
    progress = F,
    cache=T
    )
}



rafa_loc2 <- function(){
  fields <- geocodebr::setup_address_fields(
    logradouro = 'logradouro',
    numero = 'numero',
    cep = 'cep',
    bairro = 'bairro',
    municipio = 'municipio',
    estado = 'uf'
  )

  df_rafa_local2 <- geocodebr:::geocode_rafa_local2(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = ncores,
    progress = F,
    cache=T
  )
}

dani <- function(){
  fields <- geocodebr::setup_address_fields(
    logradouro = 'logradouro',
    numero = 'numero',
    cep = 'cep',
    bairro = 'bairro',
    municipio = 'municipio',
    estado = 'uf'
  )


  df_dani <- geocodebr:::geocode(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = ncores,
    progress = T
  )
}


dani_like <- function(){
  fields <- geocodebr::setup_address_fields(
    logradouro = 'logradouro',
    numero = 'numero',
    cep = 'cep',
    bairro = 'bairro',
    municipio = 'municipio',
    estado = 'uf'
  )


  df_duck_daniL <- geocodebr:::geocode_like(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = ncores,
    progress = F
  )
}

mb <- microbenchmark::microbenchmark(
  dani = dani(),
  rafa_loop = rafa_loop(),
  rafa_loc = rafa_loc(),
  rafa_loc2 = rafa_loc2(),
#  dani_L = dani_like(),
#  rafa_like = rafa_like(),
  times  = 1
)
mb
# 20 K
# Unit: seconds
#      expr      min       lq     mean   median       uq      max neval
#      dani 64.42213 67.48675 68.67454 69.02506 69.84371 73.79225    10
# rafa_loop 42.33964 45.44855 47.62467 47.87818 48.17084 56.25725    10
# rafa_verb 34.85304 44.25750 47.44078 48.59672 51.40361 55.08280    10
#  rafa_loc 28.13357 37.20932 42.52181 44.34479 47.29532 53.42182    10


bm <- bench::mark(
  dani = dani(),
  rafa_loop = rafa_loop(),
  rafa_verb = rafa_verb(),
  rafa_loc = rafa_loc(),
#  dani_L = dani_like(),
#  rafa_like = rafa_like(),
  check = F,
  iterations  = 10
)
bm

# 20 K
# expression     min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
# <bch:expr> <bch:t> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>
#   1 dani          1.1m  1.18m    0.0142   20.39MB  0.00427    10     3      11.7m <NULL> <Rprofmem>
#   2 rafa_loop    58.3s  1.02m    0.0163    2.03GB  0.0488     10    30      10.2m <NULL> <Rprofmem>
#   3 rafa_verb    58.2s  1.01m    0.0166    2.03GB  0.0498     10    30      10.1m <NULL> <Rprofmem>
#   4 rafa_loc     59.8s  1.08m    0.0156   21.54MB  0.00468    10     3      10.7m <NULL> <Rprofmem>

# 20 K
#     expression      min   median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
#   1 dani          12.6s    13.4s    0.0743    21.9MB   0.0743     5     5      1.12m <NULL> <Rprofmem>
#   2 rafa          9.35s    10.3s    0.0988      19MB   0.0988     5     5     50.58s <NULL> <Rprofmem>
#   3 dani_L        22.6s    23.3s    0.0421    17.7MB   0.0421     5     5      1.98m <NULL> <Rprofmem>
#   4 rafa_like    18.33s      19s    0.0525    19.8MB   0.0525     5     5      1.59m <NULL> <Rprofmem>


# 7 cores
# Unit: seconds
#      expr      min       lq     mean   median       uq      max neval
#      dani 60.13803 67.18672 71.85839 71.06636 75.78047 87.97580    10
# rafa_loop 53.15116 55.45122 62.12378 64.13520 65.58647 70.08501    10
# rafa_verb 51.77365 55.72707 59.13042 59.53570 61.87424 69.87290    10

# rafa_loop com 312 casos per sec com 20K casos



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



# rais --------------------------------------------------------------------

rais <- ipeadatalake::ler_rais(
  ano = 2019,
  tipo = 'estabelecimento',
  as_data_frame = F,
  geoloc = T) |>
  select("id_estab", "logradouro", "bairro", "codemun", "uf", "cep",
         'lat', 'lon', 'Addr_type', 'Match_addr') |>
  compute() |>
  dplyr::slice_sample(n = 1000000) |> # sample 50K
  filter(uf != "IG") |>
  filter(uf != "") |>
  collect()

# rais <- head(rais, n = 1000) |> collect() |> dput()
data.table::setDT(rais)

# create column number
rais[, numero := gsub("[^0-9]", "", logradouro)]

# remove numbers from logradouro
rais[, logradouro_no_numbers := gsub("\\d+", "", logradouro)]
rais[, logradouro_no_numbers := gsub(",", "", logradouro_no_numbers)]


data.table::setnames(
  rais,
  old = c('lat', 'lon'),
  new = c('lat_arcgis', 'lon_arcgis')
)

tictoc::tic()
fields <- geocodebr::setup_address_fields(
  logradouro = 'logradouro_no_numbers',
  numero = 'numero',
  cep = 'cep',
  bairro = 'bairro',
  municipio = 'codemun',
  estado = 'uf'
)

rais <- geocodebr:::geocode(
  addresses_table = rais,
  address_fields = fields,
  n_cores = 20, # 7
  progress = F
)

data.table::setnames(rais, old = 'match_type', new = 'match_type_equal')
data.table::setnames(rais, old = 'lon', new = 'lon_equal')
data.table::setnames(rais, old = 'lat', new = 'lat_equal')

rais_like <- geocodebr:::geocode_like(
  addresses_table = rais,
  address_fields = fields,
  n_cores = 20, # 7
  progress = F
)

tictoc::toc()

table(rais_like$match_type_equal, rais_like$match_type)

result_arcgis <- table(rais_like$Addr_type) / nrow(rais_like) *100
result_geocodebr <- table(rais_like$match_type) / nrow(rais_like) *100

aaaa <- table(rais_like$match_type, rais_like$Addr_type) / nrow(rais_like) *100
aaaa <- as.data.frame(aaaa)
aaaa <- subset(aaaa, Freq>0)


data.table::fwrite(aaaa, 'rais.csv', dec = ',', sep = '-')



t <- subset(rais_like, match_type=='case_09' & Addr_type==	'PointAddress')




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


cnf <- ipeadatalake::read_cnefe(year = 2022) |>
  #  filter(code_state=="41") |>
  dplyr::filter(code_muni == 4106902) |>
  dplyr::collect()


d <- cnf |>
  filter(nom_seglogr %like% "HUGO SIMAS")

'DESEMBARGADOR'


