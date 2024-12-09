library(arrow)
library(duckdb)
library(DBI)
library(dplyr)
library(data.table)

# input -------------------------------------------------------



lonlat_df <- data.table(id = 1:2,
                        lon = c(-67.81918, -51.08934, -67.8191),
                        lat = c(-9.972736, -30.05981, -9.97273)
                        )

reverse_geocode_arrow(lonlat_df, row_number = 3)

reverse_geocode_arrow <- function(df, row_number){

  # df = lonlat_df
  # row_number = 1

  # create a small range around coordinates
  data.table::setDT(df)
  df[, lon_min := lon - 0.0001]
  df[, lon_max  :=  lon + 0.0001]
  df[, lat_min := lat - 0.0001]
  df[, lat_max  :=  lat + 0.0001]

  temp_lonlat <- df[row_number,]

  lon_min = temp_lonlat$lon_min
  lon_max = temp_lonlat$lon_max
  lat_min = temp_lonlat$lat_min
  lat_max = temp_lonlat$lat_max
  lon_inp = temp_lonlat$lon
  lat_inp = temp_lonlat$lat

  # create db connection -------------------------------------------------------
  db_path <- fs::file_temp(ext = '.duckdb')
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir=db_path)

  # Load CNEFE data and write to DuckDB
  cache_dir <- 'C:/Users/user/Downloads/geocodebr_cache'
  cnefe <- arrow::open_dataset(cache_dir) |> collect()
  # cnefe <- arrow_open_dataset(geocodebr_env$cache_dir)
  # duckdb::duckdb_register_arrow(con, "cnefe", cnefe)
  duckdb::dbWriteTable(con, "cnefe", cnefe,
                       temporary = TRUE, overwrite=TRUE
                       )

  duckdb::dbExistsTable(con, 'cnefe')




  temp_cnefe <- filter(cnefe, dplyr::between(lon, lon_min, lon_max)) |>
                filter(dplyr::between(lat, lat_min, lat_max)) |>
                mutate(lon_diff = lon_inp - lon,
                       lat_diff =  lat_inp - lat) |>
                collect()


  temp_output <- temp_cnefe |>
          filter(lon_diff == min(lon_diff)) |>
          filter(lat_diff == min(lat_diff)) |>
          select(-lon_diff , -lat_diff)


  return(temp_output)

}

# query_filter_lonlat_range <- glue::glue('
#   SELECT *
#   FROM cnefe
#   WHERE lon BETWEEN {lon_min} AND {lon_max}
#   AND lat BETWEEN {lat_min} AND {lat_max};
#   ')
#
# result <- DBI::dbExecute(con, query_filter_lonlat_range)


