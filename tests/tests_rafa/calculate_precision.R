library(dplyr)

# precision ------------------------------------------------------------------

input_df <- input_table <- data.frame(
  id=666,
  logradouro = 'SQS 308 Bloco C',
  numero = NA,
  cep = '70355030',
  localidade = 'Asa sul',
  municipio = 'Brasilia',
  uf = 'DF'
)

corresp <- geocodebr::definir_campos(
  estado = 'uf',
  municipio = 'municipio',
  logradouro = 'logradouro',
  cep = 'cep',
  localidade = 'localidade'
  )

a <- geocodebr::geocode(enderecos = input_df,
                        campos_endereco = corresp,
                        resultado_completo = T,
                        resultado_sf = T)


mapview::mapview(a)


cnefe_cep <- arrow::open_dataset( geocodebr::listar_dados_cache()[3])


df <- cnefe_cep |>
  dplyr::filter(estado =='DF') |>
   dplyr::filter(cep =='70355-030') |>
  dplyr::collect()




################## calculate precision as the area in m2

get_ellipsoid_area <- function(lat_vec, lon_vec){

  # lat_vec <- dt$lat
  # lon_vec <- dt$lon
  range_lon <- max(lon_vec) - min(lon_vec)
  range_lat <- max(lat_vec) - min(lat_vec)

  range_lon <- range_lon / 2
  range_lat <- range_lat / 2

  # convert to meters
  lon_meters <- 111320 * range_lon * cos(mean(lat_vec))
  lat_meters <- 111320 * range_lat

  area <- pi * lon_meters * lat_meters
  abs(area)
  }


# concave ----------------------------------------------

devtools::load_all(".")
library(data.table)
library(dplyr)
library(sf)
library(sfheaders)
library(arrow)
library(mapview)
mapview::mapviewOptions(platform = 'leafgl')

tudo <- geocodebr::listar_dados_cache()
tudo <- tudo[7]

dt <- arrow::open_dataset( tudo ) |>
  dplyr::filter(estado == 'RJ') |>
  dplyr::filter(municipio == "RIO DE JANEIRO") |>
  # dplyr::filter(cep %in% c( "20521-470")) |> # "22620-110",
  dplyr::filter(cep %in% c("22620-110", "20521-470")) |> # "22620-110",
  dplyr::collect()

dt_sf <- sfheaders::sf_point(
  obj = dt,
  x = 'lon',
  y = 'lat',
  keep = TRUE
)
sf::st_crs(dt_sf) <- 4674

mapview::mapview(dt_sf, zcol='numero')



get_concave_area <- function(lat_vec, lon_vec){

  # lat_vec <- dt$lat
  # lon_vec <- dt$lon
  temp_matrix <- matrix( c(lon_vec, lat_vec), ncol = 2, byrow = FALSE)
  temp_sf <- sfheaders::sf_point(temp_matrix, keep = T)
  sf::st_crs(temp_sf) <- 4674

  poly <- concaveman::concaveman(points = temp_sf)
  # poly <- sf::st_convex_hull(x  = st_union(temp_sf))
  # poly <- sf::st_concave_hull(x = st_union(temp_sf), ratio = 0.5)

  sf::st_crs(poly) <- 4674

  area_m2 <- sf::st_area(poly)
  area_m2 <-  as.numeric(area_m2) |> round()

  # mapview::mapview(poly) + temp_sf

  return(area_m2)
}

# mapview(temp_sf) + poly

dt[, .(lat = mean(lat),
       lon = mean(lon),
       area_m2_concave = get_concave_area(lat, lon),
       area_m2_ellipsoid = get_ellipsoid_area(lat, lon)
       ), by = cep]

