# # open input data
# data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
# input_df <- read.csv(data_path, encoding = 'Latin-1')
#
# campos <- geocodebr::listar_campos(
#   logradouro = 'nm_logradouro',
#   numero = 'Numero',
#   cep = 'Cep',
#   localidade = 'Bairro',
#   municipio = 'nm_municipio',
#   estado = 'nm_uf'
# )
#
# df_geo <- geocodebr::geocode(
#     enderecos = input_df,
#     campos_endereco = campos,
#     n_cores = 7,
#     resultado_completo = T,
#     verboso = T
#   )
#
#
# df_rafaF$lat |> nchar() |> summary()
#
# lapply(X=df_rafaF$lat, FUN=decimalplaces) |>  unlist() |> summary()
#
# a <- subset(df_rafaF, nchar(lat) <= 6 )
#
#
#   filtered_cnefedf$lat |> nchar() |> summary()
#   lapply(X=filtered_cnefedf$lat, FUN=decimalplaces) |>  unlist() |> as.numeric() |> summary()
#
#
#
#
#
library(mapview)
library(sfheaders)

sp_muni <- geobr::read_municipality(code_muni = 3550308)


filtered_cnefe_sp <- arrow::open_dataset( geocodebr::listar_dados_cache()[11] ) |>
    dplyr::filter(estado == 'SP') |>
    dplyr::filter(municipio == "SAO PAULO") |>
    dplyr::filter(logradouro_sem_numero == "RUA IPE ROXO") |>
    dplyr::collect()

a <- subset(filtered_cnefe_sp, cep == "04896-360")
a <- subset(filtered_cnefe_sp, cep == "04896-360")

table(a$logradouro_sem_numero)
#> RUA IPE ROXO
#>          243

a <- sfheaders::sf_point(
  obj = a,
  x = 'lon',
  y = 'lat',
  keep = TRUE
)
sf::st_crs(a) <- 4674

mapview::mapview(a, zcol='localidade') + sp_muni

#   table(a$localidade)
#   table(a$logradouro_sem_numero)
#   table(a$cep)
#
#     a <- subset(filtered_cnefe_sp, localidade %like% "COLONIA")
#

library(geocodebr)
library(mapview)

df <- data.frame(
  estado = 'sp',
  municipio = 'sao paulo',
  logradouro = 'rua ipe roxo',
  numero = 1:150,
  bairro = 'vargem grande',
  cep = '04896-360'
  )

sp_muni <- geobr::read_municipality(code_muni = 3550308)

campos <- geocodebr::listar_campos(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  localidade = 'bairro',
  municipio = 'municipio',
  estado = 'estado'
)

df_geo <- geocodebr::geocode(
  enderecos = df,
  campos_endereco = campos,
  resultado_completo = T,
  verboso = T,
  resultado_sf = F
)
# nchar(df_geo$lat) # -23.85401
# df_geo

mapview::mapview(df_geo, zcol='tipo_resultado') + sp_muni

df_geo$lat
