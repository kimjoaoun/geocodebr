#' Reverse geocoding coordinates in Brazil based on CNEFE data
#'
#' @description
#' all operations done entirely in duckdb with one single join based on
#' condition between coordinates
#'
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
reverse_geocode_join <- function(input_table,
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

  download_cnefe(state = "all", progress = progress, cache = cache)


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


  # return closets -------------------------------------------------------

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



#' Reverse geocoding coordinates in Brazil based on CNEFE data
#'
#' @description
#' all operations done entirely in duckdb with one single FILTER operation followed
#' by join
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
reverse_geocode_filter <- function(input_table,
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

  download_cnefe(state = "all", progress = progress, cache = cache)


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

  query_filter_cases_nearby <- glue::glue("
    CREATE TABLE cases_nearby AS
    SELECT
      input_table_db.id, input_table_db.lat, input_table_db.lon,
      filtered_cnefe_coords.municipio,
      filtered_cnefe_coords.logradouro_sem_numero as logradouro,
      filtered_cnefe_coords.numero,
      filtered_cnefe_coords.cep,
      filtered_cnefe_coords.localidade as bairro,
      filtered_cnefe_coords.lat as lat_cnefe,
      filtered_cnefe_coords.lon as lon_cnefe
    FROM
        input_table_db, filtered_cnefe_coords
    WHERE
        input_table_db.lat_min < filtered_cnefe_coords.lat
        AND input_table_db.lat_max > filtered_cnefe_coords.lat
        AND input_table_db.lon_min < filtered_cnefe_coords.lon
        AND input_table_db.lon_max > filtered_cnefe_coords.lon"
                   )

  temp_n <- DBI::dbExecute(con, query_filter_cases_nearby)

  # find closest points -------------------------------------------------------

  query_coordinates_diff <- glue::glue(
    "ALTER TABLE cases_nearby ADD COLUMN lon_diff DOUBLE;
   ALTER TABLE cases_nearby ADD COLUMN lat_diff DOUBLE;

   UPDATE cases_nearby
   SET lon_diff = ABS(lon - lon_cnefe),
       lat_diff = ABS(lat - lat_cnefe);"
  )

  DBI::dbExecute(con, query_coordinates_diff)


  # return closets -------------------------------------------------------

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
  output[, c('lon_diff', 'lat_diff', 'rn') := NULL]

  # Disconnect from DuckDB when done
  duckdb::duckdb_unregister_arrow(con, 'filtered_cnefe_coords')
  duckdb::dbDisconnect(con, shutdown=TRUE)

  return(output)
}



#' Reverse geocoding coordinates in Brazil based on CNEFE data
#'
#' @description
#' all operations done entirely in arrow
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
reverse_geocode_arrow <- function(input_table,
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
  margin <- 0.001 # aprox 111 meters of latitude

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
  if(bbox_lon_min < bbox_brazil$xmin |
     bbox_lon_max > bbox_brazil$xmax |
     bbox_lat_min < bbox_brazil$ymin |
     bbox_lat_max > bbox_brazil$ymax) {stop(error_msg)}

  # download cnefe  -------------------------------------------------------

  # determine potential states
  bbox_states <- data.table::fread(system.file("extdata/states_bbox.csv", package = "geocodebr"))
  potential_states <- dplyr::filter(
    bbox_states,
    xmin > bbox_lon_min &
      xmax < bbox_lon_max &
      ymin > bbox_lat_min &
      ymax < bbox_lat_max)$abbrev_state


  download_cnefe(state = potential_states, progress = progress)



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

  # # create db connection -------------------------------------------------------
  # con <- create_geocodebr_db(n_cores = n_cores)
    #
    # duckdb::duckdb_register_arrow(con, "filtered_cnefe_coords",
    #                             filtered_cnefe_coords)



  # find each row in the input data -------------------------------------------------------

  # function to reverse geocode one row of the data
  reverse_geocode_single_row <- function(row_number, input_table, filtered_cnefe_coords){

    # row_number = 3

    # subset row
    temp_df <- input_table[row_number,]

    lon_min <- temp_df$lon_min
    lon_max <- temp_df$lon_max
    lat_min <- temp_df$lat_min
    lat_max <- temp_df$lat_max
    lon_inp <- temp_df$lon
    lat_inp <- temp_df$lat


    cnefe_nearby <- dplyr::filter(
        filtered_cnefe_coords,
        lon_cnefe > lon_min &
        lon_cnefe < lon_max &
        lat_cnefe > lat_min &
        lat_cnefe < lat_max) |>
      dplyr::collect()


    # find closest points
    data.table::setDT(cnefe_nearby)
    cnefe_nearby[, lon_diff := abs(lon_inp - lon_cnefe)]
    cnefe_nearby[, lat_diff := abs(lat_inp - lat_cnefe)]

    cnefe_nearest <- cnefe_nearby[, .SD[which.min(lat_diff)]]
    cnefe_nearest <- cnefe_nearest[, .SD[which.min(lon_diff)]]

    # organize output
    cnefe_nearest[, c('lon_diff', 'lat_diff') := NULL]
    temp_df[, c('lon_min', 'lon_max', 'lat_min', 'lat_max') := NULL]

    temp_output <- cbind(temp_df, cnefe_nearest)

    return(temp_output)
  }

  # apply function to all rows in the input table
  #if(n_cores==1){
    output <- pbapply::pblapply(
    X = 1:nrow(input_table),
    FUN = reverse_geocode_single_row,
    input_table = input_table,
    filtered_cnefe_coords = filtered_cnefe_coords
    )
  #}

  # if(n_cores>1){
  #
  #   # Set up the cluster with 7 cores
  #   cl <- parallel::makeCluster(n_cores)
  #
  #   # Export necessary functions to the cluster (if needed)
  #   parallel::clusterExport(cl, list("reverse_geocode_single_row",
  #                          "input_table", "filtered_cnefe_coords"))
  #
  #
  #   # Use pbapply with parallel processing
  #   output2 <- pbapply::pblapply(
  #     X = 1:nrow(input_table),
  #     FUN = reverse_geocode_single_row,
  #     input_table = input_table,
  #     filtered_cnefe_coords = filtered_cnefe_coords,
  #     cl = cl
  #   )
  #
  #   # Stop the cluster after computation
  #   parallel::stopCluster(cl)
  # }

  output <- data.table::rbindlist(output)

  return(output)
}

