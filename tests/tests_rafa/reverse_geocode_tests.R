devtools::load_all('.')
library(dplyr)


# input -------------------------------------------------------

df_coords <- data.frame(
  id = 1:6,
  lon = c(-67.83112, -67.83559, -67.81918, -43.47110, -51.08934, -67.8191),
  lat = c(-9.962392, -9.963436, -9.972736, -22.695578, -30.05981, -9.97273)
  )

df_coords <- rbind(df_coords,df_coords,df_coords,df_coords,df_coords,df_coords)
df_coords <- rbind(df_coords,df_coords,df_coords,df_coords,df_coords,df_coords)
df_coords$ID <-  1:nrow(df_coords)




# ttt <- data.frame(id=1, lat=-15.814192047159876, lon=-47.90534614672923)
# reverse_geocode(df = ttt)


m <- microbenchmark::microbenchmark(
  original_duck_dt1 = reverse_geocode(input_table = df_coords, n_cores = 1),
  # original_duck_dt7 = reverse_geocode(n_cores = 7),
  #  duck_join =  reverse_geocode_join(df_coords, n_cores = 7),
  #   duck_filter = reverse_geocode_filter(df_coords, n_cores = 7),
  farrow1 = reverse_geocode_arrow(input_table = df_coords, n_cores = 1),
  # farrow7 = reverse_geocode_arrow(cores = 7),
  times = 10
)
m

#              expr      min       lq     mean   median       uq      max neval
# original_duck_dt1 14.78498  16.63755  18.07475  17.55061  19.98599  22.36579   v10
#         duck_join 78.50424 100.18708 131.23249 108.60173 160.93168 219.31938    10
#       duck_filter 50.91032  78.22421 105.44846 110.17205 135.79430 144.71624    10
#           farrow1 13.33142  15.88934  23.48346  17.83210  26.04860  59.00520    10


#              expr      min       lq     mean   median       uq      max neval
# original_duck_dt1 189.9783 192.5622 205.3836 200.2838 211.6255 247.8410    10
# original_duck_dt7 192.2824 197.5377 209.9784 205.4112 214.1427 261.9362    10

#              expr      min       lq     mean   median       uq      max neval
# original_duck_dt1 139.5682 147.7723 157.4229 152.6838 166.6124 184.7640    10
#           farrow1 128.9925 131.5034 158.8873 143.1280 169.0749 279.8226    10
