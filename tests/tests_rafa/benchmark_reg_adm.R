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

2+2

# rais --------------------------------------------------------------------

rais <- ipeadatalake::ler_rais(
  ano = 2019,
  tipo = 'estabelecimento',
  as_data_frame = F,
  geoloc = T) |>
  select("id_estab", "logradouro", "bairro", "codemun", "uf", "cep",
         'lat', 'lon', 'Addr_type', 'Match_addr') |>
  compute() |>
  dplyr::slice_sample(n = 10000000) |> # sample 10 million
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


fields <- geocodebr::setup_address_fields(
  logradouro = 'logradouro_no_numbers',
  numero = 'numero',
  cep = 'cep',
  bairro = 'bairro',
  municipio = 'codemun',
  estado = 'uf'
)

dani <- function(){ message('dani')
  rais <- geocodebr::geocode(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    progress = T
  )
}


rafa <- function(){ message('rafa')
  rais <- geocodebr:::geocode_rafa(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    progress = T
  )
}

rafa_arrow <- function(){ message('rafa_arrow')
  rais <- geocodebr:::geocode_rafa_arrow(
    addresses_table = rais,
    address_fields = fields,
    n_cores = 20, # 7
    progress = T
  )
}

mb <- microbenchmark::microbenchmark(
  dani = dani(),
  rafa = rafa(),
  rafa_arrow = rafa_arrow(),
  times  = 5
)

mb
# 8.6 milhoes de linhas
# Unit: seconds
#       expr      min       lq     mean   median       uq      max neval
#       dani 423.3079 423.3079 423.3079 423.3079 423.3079 423.3079     1
#       rafa 542.9040 542.9040 542.9040 542.9040 542.9040 542.9040     1
# rafa_arrow 260.3829 260.3829 260.3829 260.3829 260.3829 260.3829     1











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
  #  dplyr::slice_sample(n = 50000) |> # sample 20K
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
  dplyr::collect()

setDT(cad)
cad[, id := 1:nrow(cad)]



fields_cad <- geocodebr::setup_address_fields(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  bairro = 'bairro',
  municipio = 'code_muni',
  estado = 'abbrev_state'
)


dani <- function(){ message('dani')
  rais <- geocodebr::geocode(
    addresses_table = cad,
    address_fields = fields_cad,
    n_cores = 20, # 7
    progress = T
  )
}


rafa <- function(){ message('rafa')
  rais <- geocodebr:::geocode_rafa(
    addresses_table = cad,
    address_fields = fields_cad,
    n_cores = 20, # 7
    progress = T
  )
}

rafa_arrow <- function(){ message('rafa_arrow')
  rais <- geocodebr:::geocode_rafa_arrow(
    addresses_table = cad,
    address_fields = fields_cad,
    n_cores = 20, # 7
    progress = T
  )
}

mb_cad <- microbenchmark::microbenchmark(
  dani = dani(),
  rafa = rafa(),
  rafa_arrow = rafa_arrow(),
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

