devtools::load_all('.')
library(dplyr)


# input -------------------------------------------------------

df_coords <- data.frame(
  id = 1:6,
  lon = c(-67.83112, -67.83559, -67.81918, -43.47110, -51.08934, -67.8191),
  lat = c(-9.962392, -9.963436, -9.972736, -22.695578, -30.05981, -9.97273)
  )

df_coords <- rbind(df_coords,df_coords,df_coords,df_coords,df_coords,df_coords)
df_coords$id <-  1:nrow(df_coords)

sf_coords <- sfheaders::sf_point(df_coords, x='lon', y='lat', keep = TRUE)
sf::st_crs(sf_coords) <- 4674



# ttt <- data.frame(id=1, lat=-15.814192047159876, lon=-47.90534614672923)
# reverse_geocode(df = ttt)


m <- microbenchmark::microbenchmark(
  original_duck_dt1 = reverse_geocode(input_table = df_coords, n_cores = 1),
  original_duck_dt7 = reverse_geocode(input_table = df_coords, n_cores = 7),
  duck_join =  reverse_geocode_join(input_table = df_coords, n_cores = 7),
  duck_filter = reverse_geocode_filter(input_table = df_coords, n_cores = 7),
  farrow1 = reverse_geocode_arrow(input_table = df_coords, n_cores = 1),
  farrow7 = reverse_geocode_arrow(input_table = df_coords, n_cores = 7),
  times = 10
)
m

#              expr      min        lq      mean    median        uq       max neval
# original_duck_dt1 23.39397  23.49670  24.70437  24.56972  25.29400  27.98051    10
# original_duck_dt7 23.03594  24.06859  27.13643  25.73149  29.28225  35.21632    10
#         duck_join 98.45799 108.91267 161.58998 135.87958 233.35136 272.27220    10
#       duck_filter 83.19988 109.34126 160.52207 157.55076 202.75771 261.93279    10
#           farrow1 22.12681  23.06318  25.31187  24.26889  25.28902  34.90356    10
#           farrow7 22.16991  22.97438  24.02036  23.42858  25.74699  26.30339    10

# parelizacao nao esta fazendo diferenca nem p/ original_duck_dt1 nem p/ farrow
# pode ateh prejudicar no caso de original_duck_dt1

