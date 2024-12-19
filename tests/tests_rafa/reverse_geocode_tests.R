library(arrow)
library(duckdb)
library(DBI)
library(dplyr)


# input -------------------------------------------------------

df_coords <- data.frame(
  id = 1:6,
  lon = c(-67.83112, -67.83559, -67.81918, -43.47110, -51.08934, -67.8191),
  lat = c(-9.962392, -9.963436, -9.972736, -22.695578, -30.05981, -9.97273)
  )

df_coords <- rbind(df_coords,df_coords,df_coords,df_coords,df_coords,df_coords)
df_coords <- rbind(df_coords,df_coords,df_coords,df_coords,df_coords,df_coords)
df_coords <- rbind(df_coords,df_coords,df_coords,df_coords,df_coords,df_coords)
df_coords <- rbind(df_coords,df_coords,df_coords,df_coords,df_coords,df_coords)
df_coords <- rbind(df_coords,df_coords,df_coords,df_coords,df_coords,df_coords)
df_coords <- rbind(df_coords,df_coords,df_coords,df_coords,df_coords,df_coords)
df_coords$ID <-  1:nrow(df_coords)


tictoc::tic()
a <- reverse_geocode(df_coords)
tictoc::toc()
# 37.78 sec elapsed
# 18.95 sec elapsed

tictoc::tic()
b <- reverse_geocode2(lonlat_df)
tictoc::toc()
# 37.78 sec elapsed


ttt <- data.frame(id=1, lat=-15.814192047159876, lon=-47.90534614672923)
reverse_geocode(df = ttt)
