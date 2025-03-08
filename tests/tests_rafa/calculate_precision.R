devtools::load_all(".")
library(data.table)
library(dplyr)
library(sf)
library(sfheaders)
library(arrow)
library(mapview)
sf::sf_use_s2(FALSE)
mapview::mapviewOptions(platform = 'leafgl')

tudo <- geocodebr::listar_dados_cache()
tudo <- tudo[7]

dt <- arrow::open_dataset( tudo ) |>
  dplyr::filter(estado == 'RJ') |>
  dplyr::filter(municipio == "RIO DE JANEIRO") |>
   dplyr::filter(cep %in% c( "22440-032")) |> # "22620-110",
  # dplyr::filter(cep %in% c("22620-110", "20521-470")) |> # "22620-110",
  # dplyr::filter(localidade %in% c("LEBLON")) |> # "22620-110",
  dplyr::collect()


# dt <- arrow::open_dataset( tudo ) |>
#   dplyr::filter(estado =='DF') |>
#   dplyr::filter(cep =='70355-030') |>
#   dplyr::collect()
#

dt_sf <- sfheaders::sf_point(
  obj = dt,
  x = 'lon',
  y = 'lat',
  keep = TRUE
)

sf::st_crs(dt_sf) <- 4674

mapview::mapview(dt_sf, zcol='cep')


# calculate precision as the area in m2 ----------------------------------------

## ellipsoid ----------------------------------------------

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


## concave of points ----------------------------------------------

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
  # mapview(poly) + temp_sf

  area_m2 <- sf::st_area(poly)
  area_m2 <-  as.numeric(area_m2)

  # mapview::mapview(poly) + temp_sf

  return(area_m2)
}



## concave of geos ----------------------------------------------

get_concave_area_geos <- function(lat_vec, lon_vec){

  # lat_vec <- dt$lat
  # lon_vec <- dt$lon

  # points buffer of 5.84 meters based on the best gps precision of cnefe
  # https://biblioteca.ibge.gov.br/visualizacao/livros/liv102063.pdf
  radious_meters <- 5.84
  radious_degree <- radious_meters/111320

  n_points <- length(lon_vec)
  single_point_area <- pi * radious_meters^2

  if ( n_points == 1) {
    return(single_point_area)
  }

  points <- geos::geos_make_point(
    x = lon_vec,
    y = lat_vec,
    crs = 4674
  )

  # # para agregacoes com muitos enderecos, pegar amostra aletaroria de 20%
  # if ( n_points > 200) {
  #   sample_rows <- sample(1:length(points), size = round(0.2 * length(points)))
  #   points <- points[sample_rows]
  # }

  buff <- geos::geos_buffer(
    geom = points,
    distance = radious_degree
  )

  buff <- geos::geos_unique_points(buff)
  buff <- geos::geos_make_collection(buff)
  poly <- geos::geos_concave_hull(buff, ratio  = 0.51)

  poly <- geos::geos_make_valid(poly)
  # plot(poly)

  poly <- st_as_sf(poly)
  sf::st_crs(poly) <- 4674
  # mapview(poly) + temp_sf

  # buff2 <- sf::st_buffer(x = temp_sf, dist = radious_meters)
  # buff2 <- sf::st_union(buff2)
  # sf::st_crs(buff2) <- 4674
  # mapview::mapview(buff) + buff2

  # get area
  area_m2 <- sf::st_area(poly)
  area_m2 <- as.numeric(area_m2)
  return(area_m2)
}


# mapview(temp_sf) + poly

# teste -------------------------------------------------------------------

bench::system_time({
result <- dt[, .(lat = mean(lat),
         lon = mean(lon),
         area_m2_concave = get_concave_area(lat, lon),
         area_m2_concave_geos = get_concave_area_geos(lat, lon),
         area_m2_ellipsoid = get_ellipsoid_area(lat, lon)
  ), by = cep]
})
result




fconcave <- function(dt){
  dt[, .(lat = mean(lat),
         lon = mean(lon),
         area_m2_concave = get_concave_area(lat, lon)
  ), by = cep][]
}

fconcave_geos <- function(dt){
  dt[, .(lat = mean(lat),
         lon = mean(lon),
         area_m2_concave_geos = get_concave_area_geos(lat, lon)
  ), by = cep][]
}

bench::mark(
  fconcave(dt),
  fconcave_geos(dt),
  iterations = 1,
  check = FALSE
  )

#     expression      min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result
#   1 fconcave(dt)  2.55s  2.55s     0.392    8.44MB    0.392     1     1      2.55s <NULL>
#   2 fconcave_geo… 2.68s  2.68s     0.373    4.53MB    0.373     1     1      2.68s <NULL>

library(collapse)

bench::mark(
  fconcave(dt),
  fconcave_geos(dt),

dt |>
  fgroup_by(cep) |>
  fsummarise(
           lat = fmean(lat),
           lon = fmean(lon),
           area_m2_concave = get_concave_area(lat, lon)
           ),
iterations = 10,
check = FALSE
)


#     expression      min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result
#   1 fconcave(dt)  2.61s  2.84s     0.350    4.05MB    0.629    10    18      28.6s <NULL>
#   2 fconcave_geo… 2.42s  2.54s     0.385    1.82MB    0.424    10    11        26s <NULL>
#   3 fsummarise(f… 2.67s  2.88s     0.332    4.04MB    0.597    10    18      30.1s <NULL>
