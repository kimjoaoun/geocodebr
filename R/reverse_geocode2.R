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
#' # df_addresses <- geocodebr::reverse_geocode2(
#' #   input_table = df_coords,
#' #   progress = TRUE
#' #   )
#'
reverse_geocode2 <- function(input_table,
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
    dplyr::filter(lon > bbox_lon_min &
                  lon < bbox_lon_max &
                  lat > bbox_lat_min &
                  lat < bbox_lat_max) |>
    dplyr::compute()

  duckdb::duckdb_register_arrow(con, "filtered_cnefe_coords",
                                filtered_cnefe_coords)


  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_table_db", input_table,
                       temporary = TRUE)


  # Find cases nearby -------------------------------------------------------

  query_join_cases_nearby <- glue::glue(
    "CREATE TABLE cases_nearby AS
    SELECT *
    FROM input_table_db
    LEFT JOIN filtered_cnefe_coords
    ON input_table_db.lat_min < filtered_cnefe_coords.lat
    AND input_table_db.lat_max > filtered_cnefe_coords.lat
    AND input_table_db.lon_min < filtered_cnefe_coords.lon
    AND input_table_db.lon_max > filtered_cnefe_coords.lon"
  )

  temp_n <- DBI::dbExecute(con, query_join_cases_nearby)


  # find closest points -------------------------------------------------------

  query_coordinates_diff <- glue::glue(
  "ALTER TABLE cases_nearby ADD COLUMN lon_diff DOUBLE;
   ALTER TABLE cases_nearby ADD COLUMN lat_diff DOUBLE;

   UPDATE cases_nearby
   SET lon_diff = ABS(lon - lon_1),
       lat_diff = ABS(lat - lat_1);"
   )

  DBI::dbExecute(con, query_coordinates_diff)


  # return closets ---------------

  query_return_closest <- glue::glue(
  "WITH ranked_rows AS (
    SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY lon_diff ASC, lat_diff ASC
    ) AS rn
    FROM cases_nearby
  )
  SELECT *
    FROM ranked_rows
  WHERE rn = 1;"
  )

  output <- DBI::dbGetQuery(con, query_return_closest)




  # organize output -------------------------------------------------

  data.table::setDT(output)
  output[, c('lon_min', 'lon_max', 'lat_min', 'lat_max',
             'lon_diff', 'lat_diff', 'rn') := NULL]

  data.table::setnames(
      x = output,
      old = c('lon_1', 'lat_1'),
      new = c('lon_cnefe', 'lat_cnefe')
    )





  # Disconnect from DuckDB when done
  duckdb::duckdb_unregister_arrow(con, 'filtered_cnefe_coords')
  duckdb::dbDisconnect(con, shutdown=TRUE)

  return(output)
}
