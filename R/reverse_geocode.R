#' Reverse geocoding coordinates in Brazil based on CNEFE data
#'
#' @description
#' Takes a data frame containing coordinates (latitude and longitude) and
#' returns  the address in CNEFE that is the closest to the input coordinates.
#' Latitude and longitude inputs are limited to possible values within the
#' bounding box of Brazil.
#'
#' @param input_table A data frame. It must contain the columns `'id'`,  `'lat'`, `'lon'`.
#' @template n_cores
#' @template progress
#' @template cache
#'
#' @return A `"data.frame"` object.
#' @family Reverse geocoding
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#'
#' # # input data
#' # df_coords <- data.frame(
#' #   id = 1:6,
#' #   lon = c(-67.83112, -67.83559, -67.81918, -43.47110, -51.08934, -67.8191),
#' #   lat = c(-9.962392, -9.963436, -9.972736, -22.695578, -30.05981, -9.97273)
#' # )
#' #
#' # # reverse geocode
#' # df_addresses <- geocodebr::reverse_geocode(
#' #   input_table = df_coords,
#' #   progress = TRUE
#' #   )
#'
reverse_geocode <- function(input_table,
                            n_cores = 1,
                            progress = TRUE,
                            cache = TRUE
                            ){

  # check input
  checkmate::assert_data_frame(input_table)
  checkmate::assert_logical(progress)
  checkmate::assert_number(n_cores, null.ok = TRUE)
  checkmate::assert_logical(cache)
  checkmate::assert_names(
    names(input_table),
    must.include = c("id","lon","lat"),
    .var.name = "input_table"
  )

  # prep input -------------------------------------------------------

  # 1 degree of latitude is always 111111.1 meters
  # 1 degree of longitude is  111111.1 * cos(lat)

  # create a small range around coordinates
  margin <- 0.001 # 0.0001

  data.table::setDT(input_table)
  input_table[, c("lon_min", "lon_max", "lat_min", "lat_max") :=
                .(lon - margin, lon + margin, lat - margin, lat + margin)]

  bbox_lon_min <- min(input_table$lon_min)
  bbox_lon_max <- max(input_table$lon_max)
  bbox_lat_min <- min(input_table$lat_min)
  bbox_lat_max <- max(input_table$lat_max)

  # check if input falls within Brazil
  bbox_brazil <- data.frame(
    xmin = -73.99044997,
    ymin = -33.752081270872,
    xmax = -28.83594354,
    ymax = 5.27184108017288)

  error_msg <- 'Input coordinates outside the bounding box of Brazil.'
  if(bbox_lon_min < bbox_brazil$xmin){stop(error_msg)}
  if(bbox_lon_max > bbox_brazil$xmax){stop(error_msg)}
  if(bbox_lat_min < bbox_brazil$ymin){stop(error_msg)}
  if(bbox_lat_max > bbox_brazil$ymax){stop(error_msg)}

  # download cnefe  -------------------------------------------------------

  download_cnefe(state = "all", progress = progress)


  # create db connection -------------------------------------------------------
  con <- create_geocodebr_db(n_cores = n_cores)

  # Narrow search scope in cnefe to bounding box

  filtered_cnefe_coords <- arrow::open_dataset(get_cache_dir()) |>
    dplyr::select(
      municipio,
      logradouro = logradouro_sem_numero,
      numero,
      cep,
      bairro = localidade,
      lat_cnefe = lat,
      lon_cnefe = lon
    ) |>
    dplyr::filter(lon_cnefe > bbox_lon_min &
                  lon_cnefe < bbox_lon_max &
                  lat_cnefe > bbox_lat_min &
                  lat_cnefe < bbox_lat_max) |>
    dplyr::compute()

  duckdb::duckdb_register_arrow(con, "filtered_cnefe_coords",
                                filtered_cnefe_coords)



  # find each row in the input data -------------------------------------------------------

  # function to reverse geocode one row of the data
  reverse_geocode_single_row <- function(row_number, input_table, conn){


    # row_number = 1

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
      WHERE lon_cnefe BETWEEN {lon_min} AND {lon_max}
      AND lat_cnefe BETWEEN {lat_min} AND {lat_max};
      ')

    # DBI::dbGetQuery(con, query_filter_cases_nearby)
    cnefe_nearby <- DBI::dbGetQuery(con, query_filter_cases_nearby)

    # find closest points
    data.table::setDT(cnefe_nearby)
    cnefe_nearby[, lat_diff := abs(lat_inp - lat_cnefe)]
    cnefe_nearby[, lon_diff := abs(lon_inp - lon_cnefe)]

    cnefe_nearest <- cnefe_nearby[, .SD[which.min(lat_diff)]]
    cnefe_nearest <- cnefe_nearest[, .SD[which.min(lon_diff)]]

    # organize output
    cnefe_nearest[, c('lon_diff', 'lat_diff') := NULL]
    temp_df[, c('lon_min', 'lon_max', 'lat_min', 'lat_max') := NULL]

    temp_output <- cbind(temp_df, cnefe_nearest)

    return(temp_output)
  }


  # apply function to all rows in the input table
  output <- pbapply::pblapply(
    X = 1:nrow(input_table),
    FUN = reverse_geocode_single_row,
    input_table = input_table,
    conn = con
  )

  # if(n_cores>1){
  #
  #   future::plan(future::multisession, workers = n_cores) # parallelly::availableCores()
  #
  #
  #   output <- furrr::future_map(.x = 1:nrow(input_table),
  #                               .f = reverse_geocode_single_row,
  #                               input_table = input_table,
  #                               conn = con
  #                               )
  #   }


  output <- data.table::rbindlist(output)

  # Disconnect from DuckDB when done
  duckdb::duckdb_unregister_arrow(con, 'filtered_cnefe_coords')
  duckdb::dbDisconnect(con, shutdown=TRUE)

  return(output)
}
