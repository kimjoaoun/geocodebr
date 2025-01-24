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
library(data.table)

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
  numero = 1:5000,
  bairro = 'vargem grande',
  cep = NA
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
  resultado_sf = T
)
# nchar(df_geo$lat) # -23.85401
# df_geo

mapview::mapview(df_geo, zcol='numero') + sp_muni

df_geo$lat





df <- fread("C:/Users/user/Downloads/teste_geocodebr_1km.csv")
head(df)
i <- 2
dfi <- df[i,]
dfi$tipo_resultado

bairroi <- df[i, 'NM_BAIRRO']$NM_BAIRRO
logr <- df[i, 'LOGRADOURO']$LOGRADOURO |> enderecobr::padronizar_logradouros()
num <- df[i, 'NUMERO']$NUMERO
cepi <- df[i, 'NR_CEP']$NR_CEP |> enderecobr::padronizar_ceps()
muni <- df[i, 'nm_municipio']$nm_municipio
uf <- df[i, 'nm_uf']$nm_uf

tudo <- geocodebr::listar_dados_cache()[11]

filtered_cnefe_sp <- arrow::open_dataset( geocodebr::listar_dados_cache()[6] ) |>
  dplyr::filter(estado == 'SP') |>
  dplyr::filter(municipio == "SAO PAULO") |>
  dplyr::filter(logradouro_sem_numero == logr) |>
  dplyr::filter(cep == cepi) |>
  dplyr::collect()


unique(filtered_cnefe_sp$logradouro_sem_numero)
unique(filtered_cnefe_sp$cep)
unique(filtered_cnefe_sp$localidade)
unique(filtered_cnefe_sp$numero)

a <- subset(filtered_cnefe_sp, cep == "04896-360")


campos <- geocodebr::listar_campos(
  logradouro = 'LOGRADOURO',
  numero = 'NUMERO',
  cep = 'NR_CEP',
  localidade = 'NM_BAIRRO',
  municipio = 'nm_municipio',
  estado = 'nm_uf'
)

df_geo <- geocodebr::geocode(
  enderecos = select(df[2,], -c("lat", "lon", "tipo_resultado",'precisao')),
  campos_endereco = campos,
  resultado_completo = T,
  verboso = T,
  resultado_sf = T
)


a <- sfheaders::sf_point(
  obj = filtered_cnefe_sp,
  x = 'lon',
  y = 'lat',
  keep = TRUE
)
sf::st_crs(a) <- 4674

mapview::mapview(a, zcol='localidade')



# rua ipea roxo ---------------
tudo <- geocodebr::listar_dados_cache()[11]
log_e_cep <- geocodebr::listar_dados_cache()[6]
log_e_cep <- geocodebr::listar_dados_cache()[7]


filtered_cnefe_sp <- arrow::open_dataset( tudo ) |>
  dplyr::filter(estado == 'SP') |>
  dplyr::filter(municipio == "SAO PAULO") |>
  dplyr::filter(logradouro_sem_numero == "RUA IPE ROXO") |>
  dplyr::filter(cep == "04896-360") |>
  dplyr::collect()


filtered_cnefe_sp <- arrow::open_dataset( log_e_cep ) |>
  dplyr::filter(estado == 'SP') |>
  dplyr::filter(municipio == "SAO PAULO") |>
  dplyr::filter(logradouro_sem_numero == "RUA IPE ROXO") |>
  dplyr::filter(cep == "04896-360") |>
  dplyr::collect()


setDT(filtered_cnefe_sp)

filtered_cnefe_sp[, . (lat=mean(lat), lon=mean(lon)), by= c("logradouro_sem_numero", "cep",'localidade'  )]


#' notas internas:
#' 1) criar o probabilistico e ver se corrige
#'
#' 2) se tem dois lograd com mesmo cep, pq o left join só retorna um e nao os dois?
#'  porque na hora de gerar os parquets, a gente tira a media sem a coluna de localidade
#'  e dai a media dá um resultado bizarro no meio do caminho.
#'  SE alternativamente a gente fizer pra categoria er02 o merge com o parquet
#'  que inclui o bairro, daí o left join retorna mais de um resultado. E dai
#'  poderiamos indicar caso de empate
#'

#' 1) COLÉGIO EQUIPE
#' O logradouro está com uma letra errada no dado de input. O nome correto da é
#' 'Sao Vicente de PaulO', mas o input é de PaulA. TSE e Google maps concordam.
#' geocodebr erra feio porque ainda nao tem match probabilistico por string.
#' Se corrigir o nome da rua no input, o geocodebr acerta. Mas uma proxima etapa
#' é implementar o match probabilistico mesmo.
#'
#' 2) E.E. JOSÉ DE SAN MARTIN
#' Caso parecido com o da  'RUA IPE ROXO'. O CNEFE aponta que existem endereços
#' no logradouro "RUA DELTA" e com o mesmo CEP "08071-060" em áreas completamente
#' distantes entre si. Nos bairros "JABAQUARA" e "UNIAO VILA NOVA". Como nessa
#' categoria o geobr tira a media das coordenadas de todos enderecos com mesmo
#' lograouro e cep, o resultado sai num lugar no meio do caminho e sem sentido.
#' To pensando uma solucao aqui q recupera a info do bairro, mesmo quando ele nao
#' bate com o input do usuario.
#'
#'
#' 6) EE PROF. MARIO ARMINANTE
#' as coordenadas do TSE e o geocodebr estao bem proximas (170 metros). Parece
#' correto mesmo olhando o google street view. As coordenadas do google maps
#' levam para uma outra instituição chamada 'CEU EMEF MANOEL VIEIRA DE QUEIROZ
#' FILHO'
#'
#'
#'
#'
#'


# rua delta ---------------

tudo <- geocodebr::listar_dados_cache()[11]
log_e_cep <- geocodebr::listar_dados_cache()[6]
log_e_cep <- geocodebr::listar_dados_cache()[7]

filtered_cnefe_sp <- arrow::open_dataset( tudo ) |>
  dplyr::filter(estado == 'SP') |>
  dplyr::filter(municipio == "SAO PAULO") |>
  dplyr::filter(logradouro_sem_numero == "RUA DELTA") |>
  dplyr::filter(cep == "08071-060") |>
  dplyr::collect()


filtered_cnefe_sp <- arrow::open_dataset( log_e_cep ) |>
  dplyr::filter(estado == 'SP') |>
  dplyr::filter(municipio == "SAO PAULO") |>
  dplyr::filter(logradouro_sem_numero == "RUA DELTA") |>
  dplyr::filter(cep == "08071-060") |>
  dplyr::collect()


seDT(filtered_cnefe_sp)

filtered_cnefe_sp[, . (lat=mean(lat), lon=mean(lon)), by= c("logradouro_sem_numero", "cep",'localidade'  )]
