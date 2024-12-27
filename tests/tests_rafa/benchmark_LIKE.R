

#' take-away
#' 1) incluir LIKE no campo d elogradouro melhor MUITO performance, encontrando
#' muito mais casos em cases de match 1, 2, 3 e 4
#'
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
library(tictoc)
library(dplyr)
# library(geocodebr)
# library(enderecobr)
# library(data.table)
# library(arrow)
# library(duckdb)


# open input data


data_path <- system.file("extdata/large_sample.parquet", package = "geocodebr")
input_df <- arrow::read_parquet(data_path)

input_df <- rbind(input_df,input_df,input_df)

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
ncores <- 15

rafa <- function(){
  df_duck_rafa <- geocodebr:::geocode_rafa(
    input_table = input_df,
    logradouro = "logradouro",
    numero = "numero",
    cep = "cep",
    bairro = "bairro",
    municipio = "municipio",
    estado = "uf",
    output_simple = F,
    n_cores=ncores,
    progress = F
  )
}


rafa_like <- function(){
  df_duck_rafa2 <- geocodebr:::geocode_rafa_like(
    input_table = input_df,
    logradouro = "logradouro",
    numero = "numero",
    cep = "cep",
    bairro = "bairro",
    municipio = "municipio",
    estado = "uf",
    output_simple = F,
    n_cores=ncores,
    progress = F
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


  df_duck_dani <- geocodebr:::geocode(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = ncores,
    progress = F
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
  rafa = rafa(),
  dani_L = dani_like(),
  rafa_like = rafa_like(),
  check = F,
  times  = 10
)

bm <- bench::mark(
  dani = dani(),
  rafa = rafa(),
  dani_L = dani_like(),
  rafa_like = rafa_like(),
  check = F,
  iterations  = 10
)
bm

# 20 K
#     expression      min   median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
#   1 dani          12.6s    13.4s    0.0743    21.9MB   0.0743     5     5      1.12m <NULL> <Rprofmem>
#   2 rafa          9.35s    10.3s    0.0988      19MB   0.0988     5     5     50.58s <NULL> <Rprofmem>
#   3 dani_L        22.6s    23.3s    0.0421    17.7MB   0.0421     5     5      1.98m <NULL> <Rprofmem>
#   4 rafa_like    18.33s      19s    0.0525    19.8MB   0.0525     5     5      1.59m <NULL> <Rprofmem>


# 1.6 milhoes
#     expression      min   median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
#   1 dani         27.24s   29.56s   0.0340     1.13GB  0.0238     10     7       4.9m <NULL> <Rprofmem>
#   2 rafa          1.22m    1.26m   0.0124     9.91GB  0.0211     10    17      13.5m <NULL> <Rprofmem>
#   3 dani_L        3.31m    3.83m   0.00439    1.13GB  0.00176    10     4        38m <NULL> <Rprofmem>
#   4 rafa_like     5.26m    5.34m   0.00303   15.55GB  0.00607    10    20        55m <NULL> <Rprofmem>


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
