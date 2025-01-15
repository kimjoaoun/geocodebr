devtools::load_all('.')
library(ipeadatalake)
library(dplyr)
library(data.table)
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


fields <- geocodebr::setup_address_fields(
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
    keep_matched_address =  T,
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
    keep_matched_address = F,
    progress = T
  )
}




rafaF_db <- function(){ message('rafa F')
  df_rafaF <- geocodebr:::geocode_db(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    keep_matched_address = F,
    progress = T
  )
}


rafaT_db <- function(){ message('rafa T')
  df_rafaT <- geocodebr:::geocode_db(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    keep_matched_address = T,
    progress = T
  )
}

rafaT <- function(){ message('rafa T')
  df_rafaT <- geocodebr::geocode(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    keep_matched_address = T,
    progress = T
  )
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






# cad unico --------------------------------------------------------------------

cad <- ipeadatalake::ler_cadunico(
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
cad <- cad |>
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
  dplyr::slice_sample(n = 10000000) |> # sample 20K
  dplyr::collect()


setDT(cad)

cad[, logradouro := enderecobr::padronizar_logradouros(logradouro) ]
cad[, numero := enderecobr::padronizar_numeros(numero,formato = 'integer') ]
cad[, cep := enderecobr::padronizar_ceps(cep) ]
cad[, bairro := enderecobr::padronizar_bairros(bairro) ]
cad[, code_muni := enderecobr::padronizar_municipios(code_muni) ]
cad[, abbrev_state := enderecobr::padronizar_estados(abbrev_state, formato = 'sigla') ]


fields_cad <- geocodebr::setup_address_fields(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  bairro = 'bairro',
  municipio = 'code_muni',
  estado = 'abbrev_state'
)



rafa <- function(){ message('rafa')
  cad_geo <- geocodebr::geocode(
    addresses_table = cad,
    address_fields = fields_cad,
    n_cores = 20, # 7
    progress = T
  )
}


mb_cad <- microbenchmark::microbenchmark(
  rafa = rafa(),
  times  = 5
)

mb_cad
# 43 milhoes
# Unit: seconds
#       expr       min        lq     mean   median       uq      max neval
#       dani 3803.3419 4046.2027 4928.299 4858.996 5228.297 6704.660     5
#       rafa 1096.2143 1564.2752 2961.816 3455.412 4250.800 4442.381     5
# rafa_arrow  788.2813  915.4166 1362.994 1691.666 1697.608 1721.999     5

# aprox. 465.13 soh a padronizacao dos enderecos



campos <- enderecobr::correspondencia_campos(
  logradouro = "logradouro",
  numero = "numero",
  cep = "cep",
  bairro = "bairro",
  municipio = "code_muni",
  estado = "abbrev_state"
)

system.time(
  cad_pdr <- enderecobr::padronizar_enderecos(
    enderecos = cad,
    campos_do_endereco = campos)
)



a <- table(df_geo$match_type) / nrow(df_geo)*100
a
a <- as.data.table(a)

data.table::fwrite(a, 'a.csv', dec = ',', sep = '-')

# numero
sum(a$N[which(a$V1 %like% '01|02|03|04|05')])
54.75767

# logradouro
sum(a$N[which(a$V1 %like% '01|02|03|04|05|06|07|08')])
62.745



rafaF <- function(){ message('rafa F')
  rais <- geocodebr::geocode(
    addresses_table = cad,
    address_fields = fields_cad,
    n_cores = 10, # 7
    keep_matched_address = F,
    progress = T
  )
}


rafaF_db <- function(){ message('rafa F')
  df_rafaF <- geocodebr:::geocode_db(
    addresses_table = cad,
    address_fields = fields_cad,
    n_cores = 10, # 7
    keep_matched_address = F,
    progress = T
  )
}


rafaT_db <- function(){ message('rafa T')
  df_rafaT <- geocodebr:::geocode_db(
    addresses_table = cad,
    address_fields = fields_cad,
    n_cores = 10, # 7
    keep_matched_address = T,
    progress = T
  )
}

rafaT <- function(){ message('rafa T')
  df_rafaT <- geocodebr::geocode(
    addresses_table = cad,
    address_fields = fields_cad,
    n_cores = 10, # 7
    keep_matched_address = T,
    progress = T
  )
}

dani_arrow <- function(){ message('dani')
  df_dani <- geocodebr:::geocode_dani_arrow(
    addresses_table = cad,
    address_fields = fields_cad,
    n_cores = 10,
    progress = T
  )
}


mb <- microbenchmark::microbenchmark(
  rafa_drop = rafaF(),
  # rafa_drop_db = rafaF_db(),
  rafa_keep = rafaT(),
  rafa_keep_db = rafaT_db(),
  dani_arrow = dani_arrow(),
  t = function(x){ return(2+2)},
  times  = 7
)
mb

# 2 milhoes
# Unit: seconds
#         expr       min       lq     mean   median       uq      max neval
#    rafa_drop  84.26620 120.0313 122.3185 128.3564 137.3465 141.5922     5
# rafa_drop_db  89.38189 137.4068 129.3111 137.6787 139.9067 142.1813     5
#    rafa_keep 124.30968 136.2430 162.0449 169.5411 182.9881 197.1427     5
# rafa_keep_db 179.03518 199.3445 198.3949 203.8696 204.8223 204.9028     5
#   dani_arrow  92.18470 171.5269 187.1974 208.0572 230.0147 234.2035     5

# 5 milhoes
#
# Unit: seconds
#         expr      min       lq     mean   median       uq       max neval
#    rafa_keep 647.4232 866.5022 830.6406 873.0242 882.0129  884.2402     5
# rafa_keep_db 665.9604 687.3503 821.7426 811.2217 884.4367 1059.7439     5
#   dani_arrow 368.1275 527.4584 640.7033 600.2513 829.0065  878.6727     5


bm <- bench::mark(
  rafa_drop = rafaF(),
 # rafa_drop_db = rafaF_db(),
  rafa_keep = rafaT(),
  rafa_keep_db = rafaT_db(),
  dani_arrow = dani_arrow(),
  t = function(x){ return(2+2)},
  check = F,
  iterations  = 7
)
bm

# 2 milhoes
#     expression        min   median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
#     <bch:expr>   <bch:tm> <bch:tm>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>
#   1 rafa_drop       2.24m    2.27m   0.00728    1.57GB   0.0335     5    23      11.5m <NULL> <Rprofmem>
#   2 rafa_drop_db    2.23m    2.27m   0.00730    1.61GB   0.0365     5    25      11.4m <NULL> <Rprofmem>
#   3 rafa_keep       3.32m    3.37m   0.00493    1.59GB   0.0237     5    24      16.9m <NULL> <Rprofmem>
#   4 rafa_keep_db    3.47m    3.64m   0.00459    1.62GB   0.0221     5    24      18.1m <NULL> <Rprofmem>
#   5 dani_arrow      3.56m    3.63m   0.00456    1.46GB   0.0210     5    23      18.3m <NULL> <Rprofmem>



#' take-aways:
#' manter ou dropar matched_address faz boa diferenca de tempo, mas nao de memoria
#'
