devtools::load_all('.')
library(ipeadatalake)
library(dplyr)
library(data.table)
library(enderecobr)
# library(mapview)
# library(sfheaders)
# library(sf)
# options(scipen = 999)
# mapview::mapviewOptions(platform = 'leafgl')
set.seed(42)

#' take-away
#' 1) a performance do geocodebr fica muito proxima do arcgis
#' 2) o que precisa fazer eh checar os casos em q a gente encontra com baixa
#' precisao e arcgis com alta. O que a gente pode fazer para melhorar o match?
#' Usar o LIKE logradouro na join ja melhorou muito, mas ainda daria pra melhorar?
#'
#' t <- subset(rais_like, match_type=='case_09' & Addr_type==	'PointAddress')

2+2
# stop()




# cad unico --------------------------------------------------------------------
sample_size <- 5000000

cad_con <- ipeadatalake::ler_cadunico(
  data = 202312,
  tipo = 'familia',
  as_data_frame = F,
  colunas = c("co_familiar_fam", "co_uf", "cd_ibge_cadastro",
              "no_localidade_fam", "no_tip_logradouro_fam",
              "no_tit_logradouro_fam", "no_logradouro_fam",
              "nu_logradouro_fam", "ds_complemento_fam",
              "ds_complemento_adic_fam",
              "nu_cep_logradouro_fam", "co_unidade_territorial_fam",
              "no_unidade_territorial_fam", "co_local_domic_fam")
  )

# a <- tail(cad, n = 100) |> collect()

# compose address fields
cad <- cad_con |>
  mutate(no_tip_logradouro_fam = ifelse(is.na(no_tip_logradouro_fam), '', no_tip_logradouro_fam),
         no_tit_logradouro_fam = ifelse(is.na(no_tit_logradouro_fam), '', no_tit_logradouro_fam),
         no_logradouro_fam = ifelse(is.na(no_logradouro_fam), '', no_logradouro_fam)
         ) |>
  mutate(abbrev_state = co_uf,
          code_muni = cd_ibge_cadastro,
          logradouro = paste(no_tip_logradouro_fam, no_tit_logradouro_fam, no_logradouro_fam),
          numero = nu_logradouro_fam,
          cep = nu_cep_logradouro_fam,
          bairro = no_localidade_fam) |>
  select(co_familiar_fam,
         abbrev_state,
         code_muni,
         logradouro,
         numero,
         cep,
         bairro) |>
  dplyr::compute() |>
  dplyr::slice_sample(n = sample_size) |> # sample 20K
  dplyr::collect()


# setDT(cad)
#
# cad[, logradouro := enderecobr::padronizar_logradouros(logradouro) ]
# cad[, numero := enderecobr::padronizar_numeros(numero,formato = 'integer') ]
# cad[, cep := enderecobr::padronizar_ceps(cep) ]
# cad[, bairro := enderecobr::padronizar_bairros(bairro) ]
# cad[, code_muni := enderecobr::padronizar_municipios(code_muni) ]
# cad[, abbrev_state := enderecobr::padronizar_estados(abbrev_state, formato = 'sigla') ]


cad$id <- 1:nrow(cad)

fields_cad <- geocodebr::definir_campos(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  localidade = 'bairro',
  municipio = 'code_muni',
  estado = 'abbrev_state'
)



#' Cad completo 43 milhoes
#'
#' > rafa_drop
#' process    real
#' 23.9m     17m

bench::mark(
  cadgeo <- geocodebr::geocode(
    enderecos  = cad,
    campos_endereco = fields_cad,
    resultado_completo = T,
    n_cores = 25, # 7
    verboso = T,
    resultado_sf = F,
    resolver_empates = F
  ), iterations = 1
)

# 1 milhao
# novo prob 4.44m // 66561 empates
# antg prob 15.8m // 77189 empates

# 1 milhao
# expression   min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     EMPATES
#        new 7.33m  7.33m   0.00227    1.83GB    0.173     1    76      7.33m <dt>   <Rprofmem> 71402  vs 71390
#        old 4.66m  4.66m   0.00358    1.82GB    0.200     1    56      4.66m <dt>   <Rprofmem> 70940 vs 70940



# 500K
# expression   min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     EMPATES
#       new 1.99m  1.99m   0.00839    1000MB    0.327     1    39      1.99m <dt>   <Rprofmem>  41,317
#       old 9.56m  9.56m   0.00174    1.02GB   0.0994     1    57      9.56m <dt>   <Rprofmem>  47,258


# 1 milhao
# Unit: seconds
#      expr      min       lq     mean   median       uq       max neval
# rafa_drop 50.85587 55.34562 61.64810 64.91925 66.52224  72.02584     7
# dani_drop 54.41805 62.13952 66.75066 70.87897 72.21215  73.25427     7
# rafa_keep 69.52835 80.53112 84.74141 86.21455 90.50629  95.37216     7
# dani_keep 58.81924 92.98845 92.09274 98.03399 99.34149 103.13606     7


# 2 milhoes
# Unit: seconds
#         expr       min       lq     mean   median       uq      max neval
#    rafa_drop  84.26620 120.0313 122.3185 128.3564 137.3465 141.5922     5
# rafa_drop_db  89.38189 137.4068 129.3111 137.6787 139.9067 142.1813     5
#    rafa_keep 124.30968 136.2430 162.0449 169.5411 182.9881 197.1427     5
# rafa_keep_db 179.03518 199.3445 198.3949 203.8696 204.8223 204.9028     5
#   dani_arrow  92.18470 171.5269 187.1974 208.0572 230.0147 234.2035     5




# 2 milhoes
# Unit: seconds
#         expr      min       lq     mean   median       uq      max neval
#    rafa_keep 114.4084 191.8142 199.0075 219.0545 222.9719 230.0172     7
# rafa_keep_db 201.9937 214.5410 215.6669 215.3243 218.1688 226.9311     7
#   dani_arrow 139.4406 185.4723 201.8943 220.1483 227.4886 227.7493     7

# 5 milhoes
#
# Unit: seconds
#         expr      min       lq     mean   median       uq       max neval
#    rafa_keep 647.4232 866.5022 830.6406 873.0242 882.0129  884.2402     5
# rafa_keep_db 665.9604 687.3503 821.7426 811.2217 884.4367 1059.7439     5
#   dani_arrow 368.1275 527.4584 640.7033 600.2513 829.0065  878.6727     5


bm <- bench::mark(
  rafa_keep = rafaT(),
  dani_keep = daniT(),
  check = F,
  iterations  = 5
)
bm



#' take-aways:
#' manter ou dropar matched_address faz boa diferenca de tempo, mas nao de memoria
#'





# 500 K
#     expression     min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time
#     <bch:expr> <bch:t> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>     <list>
#   1 rafa_drop       NA     NA   NA          398MB  NA          2     5         NA <NULL> <Rprofmem> <bench_tm>
#   2 dani_drop   56.89s 56.89s    0.0176     370MB   0.0352     1     2     56.89s <NULL> <Rprofmem> <bench_tm>
#   3 rafa_keep    1.42m  1.42m    0.0118     401MB   0.0471     1     4      1.42m <NULL> <Rprofmem> <bench_tm>
#   4 dani_keep    1.35m  1.35m    0.0123     374MB   0.0494     1     4      1.35m <NULL> <Rprofmem> <bench_tm>




# # time_dani1
#    user  system elapsed
#  626.68   86.05  426.64
# 1115.14   89.67  912.00
# 1695.93   92.47 1355.42
# 2280.67   93.33 1900.61
# 2255.89   82.97 1770.97
#
# # time_rafa_keep1
#    user  system elapsed
# 2060.17   79.46 1657.95
# 2122.66   78.86 1623.79
# 2094.00   78.81 1692.17
# 2131.64   79.25 1703.75
# 2088.01   81.00 1741.21
#
# # time_rafa_keep_db1
#    user  system elapsed
# 2285.82   76.58 1862.69
# 2142.54   74.02 1831.28
# 2250.08   76.30 1844.20
# 2379.26   73.30 1917.16
# 2591.64   80.29 2226.95






# 1 milhao 66666666666
#   expression     min  median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time
#   <bch:expr>   <bch>  <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>     <list>
# 1 rafa_keep    56.04s  1.06m    0.0160     889MB   0.0959     5    30      5.21m <NULL> <Rprofmem> <bench_tm>
# 1 rafa_keep_db 1.07m   1.19m    0.0141     897MB   0.102      5    36      5.9m <NULL> <Rprofmem> <bench_tm>
# 1 dani_keep    1.05m   1.3m     0.0133     807MB   0.0716     5    27      6.28m <NULL> <Rprofmem> <bench_tm>

# 1 rafa_keep        1m  1.22m    0.0138     889MB    0.105     5    38      6.04m <NULL>
# 1 rafa_keep_db  1.03m  1.30m    0.0134     847MB   0.0914     5    34      6.2m <NULL>
# 1 dani_keep     59.9s  1.31m    0.0130     807MB   0.0860     5    33      6.4m <NULL>


# 5 milhoes  66666666666
#   expression     min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time
#   <bch:expr> <bch:t> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>     <list>
# 1 rafa_keep    4.95m  7.41m   0.00233    4.37GB   0.0168     5    36      35.8m <NULL> <Rprofmem> <bench_tm>
# 1 rafa_keep_db 5.48m  8.40m   0.00208    4.45GB   0.0145     5    35      40.1m <NULL> <Rprofmem>
# 1 dani_keep    6.64m  10.3m   0.00158    4.03GB   0.0107     5    34      52.7m <NULL> <Rprofmem>



# 20 mihoes 666666666
#     expression      min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result
#     <bch:expr>  <bch:t> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list>
#   1 rafa_keep     20.6m  33.3m  0.000523    17.2GB  0.00324     5    31      2.66h <NULL>
#   1 rafa_keep_db  22.7m  36.6m  0.000464    17.5GB  0.00288     5    31         3h <NULL>
#   1 dani_keep     40.9m  1.14h  0.000242    15.8GB  0.00145     5    30      5.75h <NULL>






# checando empates no cadunico -------------------------------------------------



data.table::setDT(empate)

empate <- filter(cadgeo, empate==T)
empate <- empate[order(id)]
empate[, count := .N, by = id]
#  View(empate)

table(empate$tipo_resultado) / nrow(empate) * 100



d <- unique(empate$id)[1]

temp <- empate |>
  dplyr::filter(id == d ) #




temp <- empate |>
  dplyr::filter( tipo_resultado=='dn02') #

d <- unique(temp$id)[6]

temp <- temp |>
  dplyr::filter(id == d ) #

temp$tipo_resultado
temp$endereco_encontrado


mapview::mapview(temp, zcol='endereco_encontrado')

sf::st_distance(temp)

# nao era para ser empate
5 "da02" "da02"   mesmo rua e cep
[1] "RUA PAULO SIMOES DA COSTA, 32 (aprox) - JARDIM ANGELA, SAO PAULO - SP, 04929-140"
[2] "RUA PAULO SIMOES DA COSTA, 32 (aprox) - ALTO DO RIVIERA, SAO PAULO - SP, 04929-140"












# rais --------------------------------------------------------------------

rais <- ipeadatalake::ler_rais(
  ano = 2019,
  tipo = 'estabelecimento',
  as_data_frame = F,
  geoloc = T) |>
  select("id_estab", "logradouro", "bairro", "codemun", "uf", "cep",
         'lat', 'lon', 'Addr_type', 'Match_addr') |>
  compute() |>
  dplyr::slice_sample(n = 1000000) |> # sample 10 million
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

rais[, id := 1:nrow(rais)]

data.table::setnames(
  rais,
  old = c('lat', 'lon'),
  new = c('lat_arcgis', 'lon_arcgis')
)


head(rais)


fields <- geocodebr::definir_campos(
  logradouro = 'logradouro_no_numbers',
  numero = 'numero',
  cep = 'cep',
  bairro = 'bairro',
  municipio = 'codemun',
  estado = 'uf'
)



rafa <- function(){ message('rafa')
  rais_geo <- geocodebr::geocode(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 7,
    full_results =  T,
    progress = T
  )
}

table(rais_geo$precision) / nrow(rais_geo) *100


mb <- microbenchmark::microbenchmark(
  rafa = rafa(),
  times  = 2
)

mb
# 8.6 milhoes de linhas
# Unit: seconds
#       expr      min       lq     mean   median       uq      max neval
#       dani 423.3079 423.3079 423.3079 423.3079 423.3079 423.3079     1
#       rafa 542.9040 542.9040 542.9040 542.9040 542.9040 542.9040     1
# rafa_arrow 260.3829 260.3829 260.3829 260.3829 260.3829 260.3829     1

# com matched address e todas categorias
# Unit: seconds
# expr      min      lq    mean   median       uq      max neval
# rafa 468.2382 835.071 1275.96 1286.295 1699.844 2090.351     5



rafaF <- function(){ message('rafa F')
  rais <- geocodebr::geocode(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    full_results = F,
    progress = T
  )
  return(2+2)
}



rafaF_db <- function(){ message('rafa F')
  df_rafaF <- geocodebr:::geocode_db(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    full_results = F,
    progress = T
  )
  return(2+2)
}

rafaT_db <- function(){ message('rafa T')
  df_rafaT <- geocodebr:::geocode_db(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    full_results = T,
    progress = T
  )
  return(2+2)
}

rafaT <- function(){ message('rafa T')
  df_rafaT <- geocodebr::geocode(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    full_results = T,
    progress = T
  )
  return(2+2)
}

mb <- microbenchmark::microbenchmark(
  rafa_drop = rafaF(),
  rafa_keep = rafaT(),
  rafa_drop_db = rafaF_db(),
  rafa_keep_db = rafaT_db(),
  times  = 5
)
mb


bm <- bench::mark(
  rafa_drop = rafaF(),
  rafa_keep = rafaT(),
  rafa_drop_db = rafaF_db(),
  rafa_keep_db = rafaT_db(),
  check = F,
  iterations  = 1
)
bm


# Unit: seconds
#    expr       min        lq     mean    median       uq      max neval
#    rafa_drop  320.9953  460.6672 1274.692  685.1642 2378.397 2528.236     5
#    rafa_keep 1397.4498 2468.7379 2887.765 3072.3166 3670.223 3830.096     5
# rafa_drop_db 2387.5650 2449.5906 2527.181 2569.6456 2584.436 2644.668     5
# rafa_keep_db 2060.3775 2823.0493 3194.852 3383.1116 3485.412 4222.308     5







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

t_arc <- sfheaders::sf_point(t[1,], x = 'lon_arcgis', y = 'lat_arcgis',keep = T)
t_geo <- sfheaders::sf_point(t[1,], x = 'lon', y = 'lat',keep = T)

st_crs(t_arc) <- 4674
st_crs(t_geo) <- 4674

mapview::mapviewOptions(platform = 'mapdeck', )

mapview(t_arc) + t_geo
sf::st_distance(t_geo, t_arc)



jp <- geocodebr::get_cache_dir() |>
  geocodebr:::arrow_open_dataset()  |>
  filter(estado=="PB") |>
  filter(municipio == "JOAO PESSOA") |>
  collect()

head(jp)

subset(jp , logradouro_sem_numero %like% "DESEMBARGADOR SOUTO MAIOR")
subset(t , logradouro_no_numbers %like% "DESEMBARGADOR SOUTO MAIOR")






