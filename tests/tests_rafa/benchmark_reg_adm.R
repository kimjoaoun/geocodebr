devtools::load_all('.')
library(tictoc)
library(dplyr)
library(data.table)
library(ipeadatalake)
library(mapview)
library(sfheaders)
library(sf)

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
  dplyr::slice_sample(n = 50000) |> # sample 50K
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
  progress = T
)
tictoc::toc()


result_arcgis <- table(rais$Addr_type) / nrow(rais) *100
result_geocodebr <- table(rais$match_type) / nrow(rais) *100

aaaa <- table(rais$match_type, rais$Addr_type) / nrow(rais) *100
aaaa <- as.data.frame(aaaa)
aaaa <- subset(aaaa, Freq>0)


data.table::fwrite(aaaa, 'rais.csv', dec = ',', sep = '-')



t <- subset(rais, match_type=='case_09' & Addr_type==	'PointAddress')

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
  dplyr::slice_sample(n = 50000) |> # sample 20K
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



gc(T)

tictoc::tic()
fields <- geocodebr::setup_address_fields(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  bairro = 'bairro',
  municipio = 'code_muni',
  estado = 'abbrev_state'
)

df_geo <- geocodebr:::geocode(
  addresses_table = cad,
  address_fields = fields,
  n_cores = 20, # 7
  progress = T
)
tictoc::toc()

# 43.8 mi: 1131.28 sec elapsed (19 min)

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

