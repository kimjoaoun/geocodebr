#' Reverse geocoding coordinates based on CNEFE data
#'
#' @description
#' Takes a data frame containing coordinates (latitude and longitude) and
#' returns  the address in CNEFE that is the closest to the input coordinates.
#' Latitude and longitude inputs are limited to possible values. Latitudes must
#' be between -90 and 90 and longitudes must be between -180 and 180.
#'
#' @param input_table A data frame. It must contain the columns `'id'`, `'lon'`, `'lat'`
#' @template ncores
#' @template progress
#' @template cache
#'
#' @return A `"data.frame"` object.
#' @export
#' @family Reverse geocoding
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#'
#' # input data
#' df_coords <- data.frame(
#'   id = 1:6,
#'   lon = c(-67.83112, -67.83559, -67.81918, -43.47110, -51.08934, -67.8191),
#'   lat = c(-9.962392, -9.963436, -9.972736, -22.695578, -30.05981, -9.97273)
#' )
#'
#' # reverse geocode
#' df_addresses <- geocodebr::reverse_geocode(
#'   input_table = df_coords,
#'   progress = TRUE
#'   )
#'
reverse_geocode <- function(input_table,
                            progress = TRUE,
                            ncores = NULL,
                            cache = TRUE
                            ){

  # check input
  checkmate::assert_data_frame(input_table)
  checkmate::assert_logical(progress)
  checkmate::assert_number(ncores, null.ok = TRUE)
  checkmate::assert_logical(cache)
  checkmate::assert_names(
    names(input_table),
    must.include = c("id","lon","lat"),
    .var.name = "input_table"
  )

  # download cnefe  -------------------------------------------------------

  download_cnefe(state = c("all"), progress = progress)



  # create db connection -------------------------------------------------------
  con <- create_geocodebr_db(ncores = ncores)



  # prep input -------------------------------------------------------

  # create a small range around coordinates

  data.table::setDT(input_table)
  margin <- 0.001 # 0.0001
  input_table[, lon_min := lon - margin]
  input_table[, lon_max  :=  lon + margin]
  input_table[, lat_min := lat - margin]
  input_table[, lat_max  :=  lat + margin]


  # Narrow search scope in cnefe to bounding box

  bbox_lon_min <- min(input_table$lon_min)
  bbox_lon_max <- max(input_table$lon_max)
  bbox_lat_min <- min(input_table$lat_min)
  bbox_lat_max <- max(input_table$lat_max)


  query_filter_cnefe_coords <- glue::glue("
  CREATE TEMPORARY TABLE filtered_cnefe_coords AS
  SELECT * FROM cnefe
  WHERE lon BETWEEN {bbox_lon_min} AND {bbox_lon_max}
    AND lat BETWEEN {bbox_lat_min} AND {bbox_lat_max}
  ")

  DBI::dbExecute(con, query_filter_cnefe_coords)




  # find each row in the input data -------------------------------------------------------

  # function to reverse geocode one row of the data
  reverse_geocode_single_row <- function(df, row_number){

    # row_number = 3

    # subset row
    temp_df <- input_table[row_number,]

    lon_min <- temp_df$lon_min
    lon_max <- temp_df$lon_max
    lat_min <- temp_df$lat_min
    lat_max <- temp_df$lat_max
    lon_inp <- temp_df$lon
    lat_inp <- temp_df$lat

    # Filter cases nearby
    query_filter_cases_nearby <- glue::glue('
      SELECT *
      FROM filtered_cnefe_coords
      WHERE lon BETWEEN {lon_min} AND {lon_max}
      AND lat BETWEEN {lat_min} AND {lat_max};
      ')

    cnefe_nearby <- duckdb::dbSendQuery(con, query_filter_cases_nearby)
    cnefe_nearby <- duckdb::dbFetch(cnefe_nearby)

    # find closest point
    data.table::setDT(cnefe_nearby)
    cnefe_nearby[, lon_diff := abs(lon_inp - lon)]
    cnefe_nearby[, lat_diff := abs(lat_inp - lat)]

    cnefe_nearest <- cnefe_nearby[, .SD[which.min(lon_diff)]]
    cnefe_nearest <- cnefe_nearest[, .SD[which.min(lon_diff)]]

    # organize output
    cnefe_nearest[, c('lon_diff', 'lat_diff') := NULL]
    temp_df[, c('lon_min', 'lon_max', 'lat_min', 'lat_max') := NULL]

    data.table::setnames(
      x = temp_df,
      old = c('lon', 'lat'),
      new = c('lon_input', 'lat_input')
    )

    temp_output <- cbind(temp_df, cnefe_nearest)

    return(temp_output)
  }

  # apply function to all rows in the input table
  output <- pbapply::pblapply(
    X = 1:nrow(input_table),
    FUN = reverse_geocode_single_row,
    df=input_table
  )

  output <- data.table::rbindlist(output)

  # Disconnect from DuckDB when done
  duckdb::dbDisconnect(con, shutdown=TRUE)
  gc()

  return(output)
}
