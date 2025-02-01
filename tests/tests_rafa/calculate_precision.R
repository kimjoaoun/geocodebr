
# precision ------------------------------------------------------------------

input_df <- input_table <- data.frame(
  id=666,
  nm_logradouro = 'SQS 308 Bloco C',
  Numero = 204,
  Complemento = 'Bloco C',
  Cep = 70355030,
  Bairro = 'Asa sul',
  nm_municipio = 'Brasilia',
  nm_uf = 'DF'
)

cnefe_cep <- arrow::open_dataset( geocodebr::get_cache_dir())
df <- filter(cnefe_cep, cep =='70355-030') |> collect()



################## calculate precision as the area in m2
range_lon <- max(df$lon) - min(df$lon)
range_lat <- max(df$lat) - min(df$lat)

lon_meters <- 111320 * range_lon * cos(mean(df$lat))
lat_meters <- 111320 * range_lat

area = pi * lon_meters * lat_meters
area
##################
range_lon <- sd(df$lon) *2
range_lat <- sd(df$lat) *2




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
tudo <- tudo[10]

filt <- arrow::open_dataset( tudo ) |>
  dplyr::filter(estado == 'RJ') |>
  dplyr::filter(municipio == "RIO DE JANEIRO") |>
  dplyr::collect()

dt <- filter(filt, cep %in% c("22620-110", "20521-470"))

ccc <- filter(dt, cep=="20521-470")


get_concave_area <- function(lat_vec, lon_vec){

  lat_vec <- ccc$lat
  lon_vec <- ccc$lon
  temp_matrix <- matrix( c(lat_vec, lon_vec), ncol = 2, byrow = FALSE)
  temp_sf <- sfheaders::sf_point(temp_matrix, keep = T)
  sf::st_crs(temp_sf) <- 4674

  poly <- concaveman::concaveman(points = temp_sf)
  area_m2 <- sf::st_area(poly)
  area_m2 <-  as.numeric(area_m2) |> round()

  return(area_m2)
}

mapview(temp_sf) + poly

dt[, .(lat = mean(lat),
       lon = mean(lon),
       area_m2 = get_concave_area(lat, lon)), by = cep]

