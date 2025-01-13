#' TO DO
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
#'   - join probabilistico com fts_main_documents.match_bm25
#'   - optimize disk and parallel operations in duckdb
#'   - exceptional cases (no info on municipio input)
#'   - calcular nivel de erro
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


# addresses_table = input_df
# n_cores = 7
# ncores <- 7
# progress = T
# cache = TRUE
# keep_matched_address = F
# address_fields <- geocodebr::setup_address_fields(
#   logradouro = 'logradouro',
#   numero = 'numero',
#   cep = 'cep',
#   bairro = 'bairro',
#   municipio = 'municipio',
#   estado = 'uf')



# benchmark different approaches ------------------------------------------------------------------
ncores <- 7



fields <- geocodebr::setup_address_fields(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  bairro = 'bairro',
  municipio = 'municipio',
  estado = 'uf'
)

rafaF <- function(){ message('rafa F')
  df_rafaF <- geocodebr::geocode(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = ncores,
    keep_matched_address = F,
    progress = T
  )
}

rafaT <- function(){ message('rafa T')
  df_rafaT <- geocodebr::geocode(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = ncores,
    keep_matched_address = T,
    progress = T
  )
}




mb <- microbenchmark::microbenchmark(
  rafa_drop = rafaF(),
 rafa_keep = rafaT(),
  times  = 4
)
mb

library(profvis)
profvis({
  rafaT()
})



# 20K FRESH
# Unit: seconds
#                expr      min        lq      mean    median        uq       max neval
#                dani 9.512572 10.533384 12.133840 12.476621 13.874084 14.055381    10
#                rafa 7.492592  9.478630 10.139055 10.651887 11.185148 11.704214    10
#        rafa_db_1tab 6.010795 12.823882 15.698666 16.877027 19.802574 20.927360    10
#    rafa_db_many_tab 3.888666  4.368634  6.519762  6.546503  7.850455  9.715533    10
# rafa_arrow_many_tab 5.413124  5.452995  5.837255  5.945242  6.020612  6.348533    10

# second run
# Unit: seconds
#                expr      min        lq      mean    median        uq       max neval
#                dani 14.78138 15.823292 15.884216 15.938587 16.095994 16.554124    10
#                rafa 12.56624 12.963294 13.282075 13.240145 13.794640 13.979016    10
#        rafa_db_1tab 23.64002 24.674406 25.698825 26.092317 26.524272 26.785120    10
#    rafa_db_many_tab 10.68212 11.360587 11.476701 11.565440 11.858326 11.991334    10
# rafa_arrow_many_tab  6.40248  6.507919  6.640216  6.635447  6.699444  7.006282    10

# 3rd round
# Unit: seconds
#                expr      min        lq      mean    median        uq       max neval
#                dani 16.434306 16.690377 17.227660 16.973456 17.543389 19.05615    10
#                rafa 13.694437 14.106194 14.293463 14.261572 14.399382 15.20275    10
#        rafa_db_1tab 28.205305 29.230116 29.643057 29.660076 30.093959 30.46253    10
#    rafa_db_many_tab 12.844879 13.127918 13.369812 13.414421 13.534294 13.92704    10
# rafa_arrow_many_tab  6.538901  6.779183  7.038967  7.041431  7.310856  7.62508    10

# 4th round
# Unit: seconds
#                expr      min        lq      mean    median        uq       max neval
#                dani 15.519423 17.139488 17.654553 17.615109 18.181859 19.560416   100
#                rafa 13.801301 14.694592 15.139558 15.097101 15.545994 17.266722   100
#        rafa_db_1tab 26.722454 30.388957 31.262250 31.386270 32.028582 33.840385   100
#    rafa_db_many_tab 12.556474 13.995775 14.364552 14.311128 14.736766 15.684131   100
# rafa_arrow_many_tab  6.721275  7.141642  7.376419  7.345936  7.575968  8.398091   100


bm <- bench::mark(
  rafa_drop = rafaF(),
  rafa_keep = rafaT(),
  check = F,
  iterations  = 5
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






# small sample data ------------------------------------------------------------------


# open input data
data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
input_df <- read.csv(data_path)



# addresses_table = input_df
# address_fields = fields
# n_cores = 7
# progress = T
# cache=T


rafa_loop <- function(){
  fields <- geocodebr::setup_address_fields(
    logradouro = 'nm_logradouro',
    numero = 'Numero',
    cep = 'Cep',
    bairro = 'Bairro',
    municipio = 'nm_municipio',
    estado = 'nm_uf'
  )

  df_rafa_loop <- geocodebr::geocode(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = 7,
    progress = T,
    cache=T
  )
}





df <- left_join(select(df_rafa, c('id', 'match_type', 'precision')),
                select(df_rafa2, c('id', 'match_type', 'precision')), by='id')

data.table::setDT(df)
data.table::setnames(
  df,
  old = c('match_type.x', 'match_type.y', 'precision.x', 'precision.y'),
  new = c('match_type.d', 'match_type.p', 'precision.d', 'precision.p') )


table(df$match_type.d, df$match_type.p)




