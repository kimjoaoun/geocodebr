devtools::load_all('.')
library(dplyr)


# input data

pontos <- readRDS(
   system.file("extdata/pontos.rds", package = "geocodebr")
   )

pontos <- pontos[1:500,]

# ok reverse_geocode_arrow
# ok reverse_geocode_hybrid
# ok reverse_geocode_join
# ok reverse_geocode_filter
reverse_geocode_filter_loop

# reverse geocode
bench::system_time(
 out <-  reverse_geocode_filter_loop(
   coordenadas = pontos,
   dist_max = 1000,
   verboso = TRUE,
   cache = T,
   n_cores = 1
   )
)
View(out)


# ttt <- data.frame(id=1, lat=-15.814192047159876, lon=-47.90534614672923)
# reverse_geocode(df = ttt)

# take aways
# ok reverse_geocode_filter  # mais rapdido e eficiente, mas sem progress bar
# ok reverse_geocode_join    # igual o _filter, mas usa join
# ok reverse_geocode_hybrid  # com progress bar mas um pouco mais lento e bem mais memoria
# ok reverse_geocode_arrow   # tempo igual a _hybrid, mas usa bem mais memoria
# ok           filterloop    # disparado o mais lento, com progress e memoria media

# essa funcao pode fica muito mais rapida / eficiente se usarmos a biblioteca de
# dados espaciais do duckdb

b5 <- bench::mark(
  duck_filter4 = reverse_geocode_filter(coordenadas = pontos, dist_max = 5000, n_cores = 4),
  duck_filter_loop4 = reverse_geocode_filter_loop(coordenadas = pontos, dist_max = 5000, n_cores = 4),
  # duck_join4 =  reverse_geocode_join(coordenadas = pontos, dist_max = 1000, n_cores = 4),
  # arrow4 = reverse_geocode_arrow(coordenadas = pontos, dist_max = 5000, n_cores = 4),
   hybrid4 = reverse_geocode_hybrid(coordenadas = pontos, dist_max = 5000, n_cores = 4),
  iterations = 5,
  check = F
)
b5

# # 500 pontos
#     expression           min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
#     <bch:expr>        <bch:> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>
#   1 duck_filter4       1.54m  1.62m   0.0101    221.5MB  0.0423      5    21      8.28m <NULL> <Rprofmem>
#   2 duck_filter_loop4  5.35m  6.88m   0.00255    14.5MB  0.00357     5     7      32.7m <NULL> <Rprofmem>
#   3 hybrid4            2.41m  2.47m   0.00663    34.5MB  0.302       5   228     12.56m <NULL> <Rprofmem>

# # 1000 pontos
#     expression   min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
#     <bch:expr> <bch> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>
#   1 duck_filt… 2.97m  2.97m   0.00560    11.3MB    0         1     0      2.97m <NULL> <Rprofmem>
#   2 duck_join4 3.03m  3.03m   0.00550    11.5MB    0         1     0      3.03m <NULL> <Rprofmem>
#   3 arrow4     4.27m  4.27m   0.00391   240.1MB    0.316     1    81      4.27m <NULL> <Rprofmem>
#   4 hybrid4    4.24m  4.24m   0.00393   122.4MB    0.110     1    28      4.24m <NULL> <Rprofmem>
#   4 filterloop 10.7m 11.27m   0.00146    19.8MB  0.00195     3     4     34.19m <NULL> <Rprofmem> <bench_tm [3]> <tibble>



# parelizacao nao esta fazendo diferenca nem p/ original_duck_dt1 nem p/ farrow
# pode ateh prejudicar no caso de original_duck_dt1


m1 <- microbenchmark::microbenchmark(
  duck_filter4 = reverse_geocode_filter(coordenadas = pontos, dist_max = 1000, n_cores = 4),
  duck_join4 =  reverse_geocode_join(coordenadas = pontos, dist_max = 1000, n_cores = 4),
  arrow4 = reverse_geocode_arrow(coordenadas = pontos, dist_max = 1000, n_cores = 4),
  hybrid4 = reverse_geocode_hybrid(coordenadas = pontos, dist_max = 1000, n_cores = 4),
  times = 5
)
m1
# # 1000 pontos
# Unit: seconds
# expr              min       lq     mean   median       uq      max neval
# duck_filter4 164.2226 184.9813 196.7560 185.3038 223.7968 225.4755     5
# duck_join4   168.5803 181.8956 197.7216 188.9110 222.6354 226.5857     5
# arrow4       258.2942 265.0396 269.4309 269.8658 272.9294 281.0256     5
# hybrid4      257.4353 258.4068 260.0735 258.6678 261.7325 264.1252     5


#' add id to input
#' create output table
#' reduce cnefe global scope
#' register on duckdb
#' overall search with 100 metros
#' atualiza input
#' overall search with 500 metros
#' atualiza input
#' overall search with 1000 metros


# for (threshol in c(100, 500, 1000)){
#
#   # find cases nearby
#   query_join_cases_nearby <- glue::glue(
#     "insert TABLE cases_nearby AS
#     SELECT *
#     FROM coordenadas_db
#     LEFT JOIN filtered_cnefe_coords
#     ON coordenadas_db.lat_min < filtered_cnefe_coords.lat
#     AND coordenadas_db.lat_max > filtered_cnefe_coords.lat
#     AND coordenadas_db.lon_min < filtered_cnefe_coords.lon
#     AND coordenadas_db.lon_max > filtered_cnefe_coords.lon"
#   )
#
#
#
#   # update input table to remove cases found
#
#
#
# }



# gerate sample points for reverse geocode -------------------------------------

library(geobr)
library(sf)
library(ggplot2)

br <- geobr::read_state(code_state = 'MG')
br <- geobr::read_urban_concentrations() |>
  filter(abbrev_state %in% c('ES', 'AP'))

set.seed(42)
n_size <- 1000
coordenadas <- sf::st_sample(br, size = n_size)

coordenadas <- st_as_sf(coordenadas)
coordenadas$id <- 1:n_size
coordenadas <- sf::st_set_geometry(coordenadas, 'geometry')

coordenadas <- coordenadas |> dplyr::select(id, geometry)
head(coordenadas)


ggplot() +
  geom_sf(data=br) +
  geom_sf(data=coordenadas)


saveRDS(coordenadas, './inst/extdata/pontos.rds')





# gerate states bbox -----------------------------------------------------------

states <- geobr::read_state(simplified = FALSE)

bbox_states2 <- states |>
  group_by(abbrev_state) |>
  mutate(xmin = sf::st_bbox(geom)[1] |> round(7),
         ymin = sf::st_bbox(geom)[2] |> round(7),
         xmax = sf::st_bbox(geom)[3] |> round(7),
         ymax = sf::st_bbox(geom)[4] |> round(7)
  ) |>
  sf::st_drop_geometry() |>
  select(abbrev_state, xmin, ymin, xmax, ymax)

data.table::setDT(bbox_states2)
bbox_states2$xmin |> nchar()

saveRDS(bbox_states2, './inst/extdata/states_bbox.rds')


devtools::load_all('.')
library(dplyr)

#' incluir parametro
#' @output_level = c('address', 'locality', 'municipality', 'state')











pontos <- readRDS(
  system.file("extdata/pontos.rds", package = "geocodebr")
)

bench::mark(
  # duck_filter1 = reverse_geocode_filter(coordenadas = pontos, dist_max = 2000, n_cores = 1),
  # duck_filter8 = reverse_geocode_filter(coordenadas = pontos, dist_max = 2000, n_cores = 8),
  # duck_join1 =  reverse_geocode_join(coordenadas = pontos, dist_max = 2000, n_cores = 1),
  # duck_join8 =  reverse_geocode_join(coordenadas = pontos, dist_max = 2000, n_cores = 8),
  hybrid1 = reverse_geocode_hybrid(coordenadas = pontos, dist_max = 2000, n_cores = 1),
  hybrid8 = reverse_geocode_hybrid(coordenadas = pontos, dist_max = 2000, n_cores = 8),
  iterations = 5,
  check = F
)

# 1000 pontos
#     expression      min   median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
#     <bch:expr>   <bch:>   <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>
# 1 duck_filter1    3.19m    3.31m   0.00490      75MB  0.0108      5    11        17m <NULL> <Rprofmem>
# 2 duck_filter8    2.93m    3.04m   0.00516      51MB  0.00723     5     7      16.1m <NULL> <Rprofmem>
#
# 1 duck_join1      2.93m    3.54m   0.00475    76.2MB  0.00854     5     9      17.6m <NULL> <Rprofmem>
# 2 duck_join8      3.62m    4.05m   0.00407    51.2MB  0.00651     5     8      20.5m <NULL> <Rprofmem>
#
# 1 hybrid1         5.13m     5.9m   0.00277    88.3MB    0.262     5   473      30.1m <NULL> <Rprofmem>
# 2 hybrid8         5.09m    5.19m   0.00321      66MB    0.307     5   478      25.9m <NULL> <Rprofmem>
