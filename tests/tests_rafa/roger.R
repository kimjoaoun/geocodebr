# roger

#' numero de empates nao esta estavel. Por que ?
#' resolver empates afeta tipo de match? nao deveria

library(data.table)
library(geocodebr)
library(dplyr)
library(arrow)
library(mapview)
mapview::mapviewOptions(platform = 'leafgl')

# remotes::install_github("ipeaGIT/geocodebr@probabilistic_matching")

file <- 'L:/# RAFAEL HENRIQUE MORAES PEREIRA #/parcerias/202410_geocode_rogerio_armados/data/reports_data_fixed_for_geocoding/reports_data_fixed_for_geocoding.parquet'
# df_padrao <- arrow::read_parquet('./data/reports_data_fixed_for_geocoding/reports_data_fixed_for_geocoding.parquet')
df_padrao <- arrow::read_parquet(file)

df_padrao$den_texto <- NULL


data.table::setDT(df_padrao)

summary(df_padrao$numero)

# a <- table(df_padrao$bairro) |> as.data.frame()


# data.table::fwrite(df_padrao, './data/reports_data_fixed_for_geocoding/reports_data_fixed_for_geocoding.csv')



# geocode {geocodebr} ---------------------------------------------------------------
df_padrao$id <- 1:nrow(df_padrao)

campos <- geocodebr::definir_campos(
  logradouro = 'logradouro',
  numero = 'numero',
  # cep = ,
  localidade = 'bairro',
  municipio = 'munic',
  estado = 'uf'
)

bench::system_time(
  df_geo <- geocodebr::geocode(
    enderecos = df_padrao,
    campos_endereco = campos,
    resultado_completo = TRUE,
    resolver_empates = T,
    verboso = T,
    n_cores = 30
  )
)


# saveRDS(ends_geolocalizados2, '//storage6/usuarios/# RAFAEL HENRIQUE MORAES PEREIRA #/parcerias/202410_geocode_rogerio_armados/data/enderecos_geocoded_geocodebr.rds')
saveRDS(df_geo, 'L:/# RAFAEL HENRIQUE MORAES PEREIRA #/parcerias/202410_geocode_rogerio_armados/data/enderecos_geocoded_probabilistico_desempatados.rds')

