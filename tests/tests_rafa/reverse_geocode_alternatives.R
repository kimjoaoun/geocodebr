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
reverse_geocode_join <- function(coordenadas,
                                 dist_max = 1000,
                                 verboso = TRUE,
                                 cache = TRUE,
                                 n_cores = 1){

  # check input
  checkmate::assert_logical(verboso)
  checkmate::assert_number(n_cores)
  checkmate::assert_logical(cache)
  checkmate::assert_class(coordenadas, 'sf')

  epsg <- sf::st_crs(coordenadas)$epsg
  if (epsg != 4674) { stop('Dados de input precisam estar com projeção geográfica SIRGAS 2000, EPSG 4674')}


  # prep input -------------------------------------------------------

  # converte para data.frame
  coords <- sfheaders::sf_to_df(coordenadas, fill = TRUE)
  data.table::setDT(coords)
  coords[, c('sfg_id', 'point_id') := NULL]
  data.table::setnames(coords, old = c('x', 'y'), new = c('lon', 'lat'))

  # create temp id
  coords[, tempidgeocodebr := 1:nrow(coords) ]

  # convert max_dist to degrees
  # 1 degree of latitude is always 111320 meters
  margin_lat <- dist_max / 111320

  # 1 degree of longitude is 111320 * cos(lat)
  coords[, c("lat_min", "lat_max") := .(lat - margin_lat, lat + margin_lat)]

  coords[, c("lon_min", "lon_max") := .(lon - dist_max / 111320 * cos(lat),
                                        lon + dist_max / 111320 * cos(lat))
  ]

  # get bounding box around input points
  # using a range of max dist around input points
  bbox_lat_min <- min(coords$lat_min)
  bbox_lat_max <- max(coords$lat_max)
  bbox_lon_min <- min(coords$lon_min)
  bbox_lon_max <- max(coords$lon_max)


  # check if input falls within Brazil
  bbox_brazil <- data.frame(
    xmin = -73.99044997,
    ymin = -33.75208127,
    xmax = -28.83594354,
    ymax =   5.27184108
  )

  error_msg <- 'Coordenadas de input localizadas fora do bounding box do Brasil.'
  if(bbox_lon_min < bbox_brazil$xmin |
     bbox_lon_max > bbox_brazil$xmax |
     bbox_lat_min < bbox_brazil$ymin |
     bbox_lat_max > bbox_brazil$ymax) { stop(error_msg) }


  # download cnefe  -------------------------------------------------------

  # downloading cnefe
  cnefe_dir <- download_cnefe(
    verboso = verboso,
    cache = cache
  )


  # limita escopo de busca aos estados  -------------------------------------------------------

  # determine potential states
  bbox_states <- readRDS(system.file("extdata/states_bbox.rds", package = "geocodebr"))
  potential_states <- dplyr::filter(
    bbox_states,
    xmin >= bbox_lon_min |
      xmax <= bbox_lon_max |
      ymin >= bbox_lat_min |
      ymax <= bbox_lat_max)$abbrev_state

  # Load CNEFE data and filter it to include only states
  # present in the input table, reducing the search scope
  # Narrow search global scope of cnefe to bounding box
  path_to_parquet <- paste0(listar_pasta_cache(), "/municipio_logradouro_numero_cep_localidade.parquet")
  filtered_cnefe <- arrow_open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% potential_states) |>
    dplyr::filter(lon >= bbox_lon_min &
                    lon <= bbox_lon_max &
                    lat >= bbox_lat_min &
                    lat <= bbox_lat_max) |>
    dplyr::compute()


  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_table_db", coords,
                       temporary = TRUE)




  # Find cases nearby -------------------------------------------------------

  query_join_cases_nearby <- glue::glue(
    "SELECT
        input_table_db.*,
        filtered_cnefe.endereco_completo,
        filtered_cnefe.estado,
        filtered_cnefe.municipio,
        filtered_cnefe.logradouro,
        filtered_cnefe.numero,
        filtered_cnefe.cep,
        filtered_cnefe.localidade,
        filtered_cnefe.lat AS lat_cnefe,
        filtered_cnefe.lon AS lon_cnefe
      FROM input_table_db
      LEFT JOIN filtered_cnefe
        ON input_table_db.lat_min <  filtered_cnefe.lat
        AND input_table_db.lat_max > filtered_cnefe.lat
        AND input_table_db.lon_min < filtered_cnefe.lon
        AND input_table_db.lon_max > filtered_cnefe.lon;"
  )

  output <- DBI::dbGetQuery(con, query_join_cases_nearby)


  # organize output -------------------------------------------------

  data.table::setDT(output)
  output[, c('lon_min', 'lon_max', 'lat_min', 'lat_max') := NULL]

  # find the closest point
  output[, distancia_metros := dt_haversine(lat,lon , lat_cnefe, lon_cnefe)]
  output <- output[output[, .I[distancia_metros == min(distancia_metros)], by = tempidgeocodebr]$V1]

  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")
  duckdb::dbDisconnect(con)

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
reverse_geocode_filter <- function(coordenadas,
                                 dist_max = 1000,
                                 verboso = TRUE,
                                 cache = TRUE,
                                 n_cores = 1){

  # check input
  checkmate::assert_logical(verboso)
  checkmate::assert_number(n_cores)
  checkmate::assert_logical(cache)
  checkmate::assert_class(coordenadas, 'sf')

  epsg <- sf::st_crs(coordenadas)$epsg
  if (epsg != 4674) { stop('Dados de input precisam estar com projeção geográfica SIRGAS 2000, EPSG 4674')}


  # prep input -------------------------------------------------------

  # converte para data.frame
  coords <- sfheaders::sf_to_df(coordenadas, fill = TRUE)
  data.table::setDT(coords)
  coords[, c('sfg_id', 'point_id') := NULL]
  data.table::setnames(coords, old = c('x', 'y'), new = c('lon', 'lat'))

  # create temp id
  coords[, tempidgeocodebr := 1:nrow(coords) ]

  # convert max_dist to degrees
  # 1 degree of latitude is always 111320 meters
  margin_lat <- dist_max / 111320

  # 1 degree of longitude is 111320 * cos(lat)
  coords[, c("lat_min", "lat_max") := .(lat - margin_lat, lat + margin_lat)]

  coords[, c("lon_min", "lon_max") := .(lon - dist_max / 111320 * cos(lat),
                                        lon + dist_max / 111320 * cos(lat))
  ]

  # get bounding box around input points
  # using a range of max dist around input points
  bbox_lat_min <- min(coords$lat_min)
  bbox_lat_max <- max(coords$lat_max)
  bbox_lon_min <- min(coords$lon_min)
  bbox_lon_max <- max(coords$lon_max)


  # check if input falls within Brazil
  bbox_brazil <- data.frame(
    xmin = -73.99044997,
    ymin = -33.75208127,
    xmax = -28.83594354,
    ymax =   5.27184108
  )

  error_msg <- 'Coordenadas de input localizadas fora do bounding box do Brasil.'
  if(bbox_lon_min < bbox_brazil$xmin |
     bbox_lon_max > bbox_brazil$xmax |
     bbox_lat_min < bbox_brazil$ymin |
     bbox_lat_max > bbox_brazil$ymax) { stop(error_msg) }


  # download cnefe  -------------------------------------------------------

  # downloading cnefe
  cnefe_dir <- download_cnefe(
    verboso = verboso,
    cache = cache
  )


  # limita escopo de busca aos estados  -------------------------------------------------------

  # determine potential states
  bbox_states <- readRDS(system.file("extdata/states_bbox.rds", package = "geocodebr"))
  potential_states <- dplyr::filter(
    bbox_states,
    xmin >= bbox_lon_min |
      xmax <= bbox_lon_max |
      ymin >= bbox_lat_min |
      ymax <= bbox_lat_max)$abbrev_state

  # Load CNEFE data and filter it to include only states
  # present in the input table, reducing the search scope
  # Narrow search global scope of cnefe to bounding box
  path_to_parquet <- paste0(listar_pasta_cache(), "/municipio_logradouro_numero_cep_localidade.parquet")
  filtered_cnefe <- arrow_open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% potential_states) |>
    dplyr::filter(lon >= bbox_lon_min &
                    lon <= bbox_lon_max &
                    lat >= bbox_lat_min &
                    lat <= bbox_lat_max) |>
    dplyr::compute()


  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_table_db", coords,
                       temporary = TRUE)




  # Find cases nearby -------------------------------------------------------

  query_filter_cases_nearby <- glue::glue(
    "SELECT
        input_table_db.*,
        filtered_cnefe.endereco_completo,
        filtered_cnefe.estado,
        filtered_cnefe.municipio,
        filtered_cnefe.logradouro,
        filtered_cnefe.numero,
        filtered_cnefe.cep,
        filtered_cnefe.localidade,
        filtered_cnefe.lat AS lat_cnefe,
        filtered_cnefe.lon AS lon_cnefe
      FROM
        input_table_db, filtered_cnefe
      WHERE
        input_table_db.lat_min <  filtered_cnefe.lat
        AND input_table_db.lat_max > filtered_cnefe.lat
        AND input_table_db.lon_min < filtered_cnefe.lon
        AND input_table_db.lon_max > filtered_cnefe.lon;"
  )

  output <- DBI::dbGetQuery(con, query_filter_cases_nearby)


  # organize output -------------------------------------------------

  data.table::setDT(output)
  output[, c('lon_min', 'lon_max', 'lat_min', 'lat_max') := NULL]

  # find the closest point
  output[, distancia_metros := dt_haversine(lat,lon , lat_cnefe, lon_cnefe)]
  output <- output[output[, .I[distancia_metros == min(distancia_metros)], by = tempidgeocodebr]$V1]

  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")
  duckdb::dbDisconnect(con)

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
reverse_geocode_arrow <- function(coordenadas,
                                      dist_max = 1000,
                                      verboso = TRUE,
                                      cache = TRUE,
                                      n_cores = 1){

  # check input
  checkmate::assert_logical(verboso)
  checkmate::assert_number(n_cores)
  checkmate::assert_logical(cache)
  checkmate::assert_class(coordenadas, 'sf')

  epsg <- sf::st_crs(coordenadas)$epsg
  if (epsg != 4674) { stop('Dados de input precisam estar com projeção geográfica SIRGAS 2000, EPSG 4674')}


  # prep input -------------------------------------------------------

  # converte para data.frame
  coords <- sfheaders::sf_to_df(coordenadas, fill = TRUE)
  data.table::setDT(coords)
  coords[, c('sfg_id', 'point_id') := NULL]
  data.table::setnames(coords, old = c('x', 'y'), new = c('lon', 'lat'))

  # create temp id
  coords[, tempidgeocodebr := 1:nrow(coords) ]

  # convert max_dist to degrees
  # 1 degree of latitude is always 111320 meters
  margin_lat <- dist_max / 111320

  # 1 degree of longitude is 111320 * cos(lat)
  coords[, c("lat_min", "lat_max") := .(lat - margin_lat, lat + margin_lat)]

  coords[, c("lon_min", "lon_max") := .(lon - dist_max / 111320 * cos(lat),
                                        lon + dist_max / 111320 * cos(lat))
  ]

  # get bounding box around input points
  # using a range of max dist around input points
  bbox_lat_min <- min(coords$lat_min)
  bbox_lat_max <- max(coords$lat_max)
  bbox_lon_min <- min(coords$lon_min)
  bbox_lon_max <- max(coords$lon_max)


  # check if input falls within Brazil
  bbox_brazil <- data.frame(
    xmin = -73.99044997,
    ymin = -33.75208127,
    xmax = -28.83594354,
    ymax =   5.27184108
  )

  error_msg <- 'Coordenadas de input localizadas fora do bounding box do Brasil.'
  if(bbox_lon_min < bbox_brazil$xmin |
     bbox_lon_max > bbox_brazil$xmax |
     bbox_lat_min < bbox_brazil$ymin |
     bbox_lat_max > bbox_brazil$ymax) { stop(error_msg) }


  # download cnefe  -------------------------------------------------------

  # downloading cnefe
  cnefe_dir <- download_cnefe(
    verboso = verboso,
    cache = cache
  )


  # limita escopo de busca aos estados  -------------------------------------------------------

  # determine potential states
  bbox_states <- readRDS(system.file("extdata/states_bbox.rds", package = "geocodebr"))
  potential_states <- dplyr::filter(
    bbox_states,
    xmin >= bbox_lon_min |
      xmax <= bbox_lon_max |
      ymin >= bbox_lat_min |
      ymax <= bbox_lat_max)$abbrev_state

  # Load CNEFE data and filter it to include only states
  # present in the input table, reducing the search scope
  path_to_parquet <- paste0(listar_pasta_cache(), "/municipio_logradouro_numero_cep_localidade.parquet")
  filtered_cnefe <- arrow_open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% potential_states) |>
    dplyr::filter(lon >= bbox_lon_min &
                    lon <= bbox_lon_max &
                    lat >= bbox_lat_min &
                    lat <= bbox_lat_max) |>
    dplyr::compute()


  # Narrow search global scope of cnefe to bounding box
  filtered_cnefe_coords <- filtered_cnefe |>
    dplyr::select(
      endereco_completo,
      estado,
      municipio,
      logradouro,
      numero,
      cep,
      localidade,
      lat_cnefe = lat,
      lon_cnefe = lon
      )



  # find each row in the input data -------------------------------------------------------

  # function to reverse geocode one row of the data
  reverse_geocode_single_row <- function(
    row_number,
    coords,
    filtered_cnefe_coords,
    dist_max){

    # row_number = 3

    # subset row
    temp_df <- coords[row_number,]

    lat_min <- temp_df$lat
    lat_max <- temp_df$lat
    lon_min <- temp_df$lon
    lon_max <- temp_df$lon

    cnefe_nearby <- filtered_cnefe_coords |>
      dplyr::filter(
        lon_cnefe >= lon_min &
          lon_cnefe <= lon_max &
          lat_cnefe >= lat_min &
          lat_cnefe <= lat_max) |>
      dplyr::collect()

    cnefe_nearest <- cbind(temp_df, cnefe_nearby)

    # find the closest point
    if (any(!is.na(cnefe_nearest$lat_cnefe))) {
      cnefe_nearest[, distancia_metros := dt_haversine(lat,lon , lat_cnefe, lon_cnefe)]
      cnefe_nearest <- cnefe_nearest[cnefe_nearest[, .I[distancia_metros == min(distancia_metros)], by = tempidgeocodebr]$V1]
      cnefe_nearest <- cnefe_nearest[cnefe_nearest[, .I[1], by = tempidgeocodebr]$V1]
    }

    return(cnefe_nearest)
  }

  # apply function to all rows in the input table

  #if(n_cores==1){
  output <- pbapply::pblapply(
    X = 1:nrow(coords),
    FUN = reverse_geocode_single_row,
    coords = coords,
    filtered_cnefe_coords = filtered_cnefe_coords,
    dist_max = dist_max
  )


  output <- data.table::rbindlist(output, fill = TRUE)

  # # find the closest point
  # output[, distancia_metros := dt_haversine(lat,lon , lat_cnefe, lon_cnefe)]
  # output <- output[output[, .I[distancia_metros == min(distancia_metros)], by = tempidgeocodebr]$V1]
  # output <- output[output[, .I[1], by = tempidgeocodebr]$V1]


  return(output)
}




reverse_geocode_hybrid <- function(coordenadas,
                                   dist_max = 1000,
                                   verboso = TRUE,
                                   cache = TRUE,
                                   n_cores = 1){

  # check input
  checkmate::assert_logical(verboso)
  checkmate::assert_number(n_cores)
  checkmate::assert_logical(cache)
  checkmate::assert_class(coordenadas, 'sf')

  epsg <- sf::st_crs(coordenadas)$epsg
  if (epsg != 4674) { stop('Dados de input precisam estar com projeção geográfica SIRGAS 2000, EPSG 4674')}


  # prep input -------------------------------------------------------

  # converte para data.frame
  coords <- sfheaders::sf_to_df(coordenadas, fill = TRUE)
  data.table::setDT(coords)
  coords[, c('sfg_id', 'point_id') := NULL]
  data.table::setnames(coords, old = c('x', 'y'), new = c('lon', 'lat'))

  # create temp id
  coords[, tempidgeocodebr := 1:nrow(coords) ]

  # convert max_dist to degrees
  # 1 degree of latitude is always 111320 meters
  margin_lat <- dist_max / 111320

  # 1 degree of longitude is 111320 * cos(lat)
  coords[, c("lat_min", "lat_max") := .(lat - margin_lat, lat + margin_lat)]

  coords[, c("lon_min", "lon_max") := .(lon - dist_max / 111320 * cos(lat),
                                        lon + dist_max / 111320 * cos(lat))
  ]

  # get bounding box around input points
  # using a range of max dist around input points
  bbox_lat_min <- min(coords$lat_min)
  bbox_lat_max <- max(coords$lat_max)
  bbox_lon_min <- min(coords$lon_min)
  bbox_lon_max <- max(coords$lon_max)


  # check if input falls within Brazil
  bbox_brazil <- data.frame(
    xmin = -73.99044997,
    ymin = -33.75208127,
    xmax = -28.83594354,
    ymax =   5.27184108
  )

  error_msg <- 'Coordenadas de input localizadas fora do bounding box do Brasil.'
  if(bbox_lon_min < bbox_brazil$xmin |
     bbox_lon_max > bbox_brazil$xmax |
     bbox_lat_min < bbox_brazil$ymin |
     bbox_lat_max > bbox_brazil$ymax) { stop(error_msg) }


  # download cnefe  -------------------------------------------------------

  # downloading cnefe
  cnefe_dir <- download_cnefe(
    verboso = verboso,
    cache = cache
  )


  # limita escopo de busca aos estados  -------------------------------------------------------

  # determine potential states
  bbox_states <- readRDS(system.file("extdata/states_bbox.rds", package = "geocodebr"))
  potential_states <- dplyr::filter(
    bbox_states,
    xmin >= bbox_lon_min |
      xmax <= bbox_lon_max |
      ymin >= bbox_lat_min |
      ymax <= bbox_lat_max)$abbrev_state

  # Load CNEFE data and filter it to include only states
  # present in the input table, reducing the search scope
  # Narrow search global scope of cnefe to bounding box
  path_to_parquet <- paste0(listar_pasta_cache(), "/municipio_logradouro_numero_cep_localidade.parquet")
  filtered_cnefe <- arrow_open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% potential_states) |>
    dplyr::filter(lon >= bbox_lon_min &
                    lon <= bbox_lon_max &
                    lat >= bbox_lat_min &
                    lat <= bbox_lat_max) |>
    dplyr::compute()


  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)


  # find each row in the input data -------------------------------------------------------

  # function to reverse geocode one row of the data
  reverse_geocode_single_row <- function(
    con,
    row_number,
    coords,
    dist_max){

    # row_number = 3

    # subset row
    temp_df <- coords[row_number,]

    lat_min <- temp_df$lat
    lat_max <- temp_df$lat
    lon_min <- temp_df$lon
    lon_max <- temp_df$lon


    # get cnefe points nearby
    query_get_nearby <- glue::glue(
      "SELECT filtered_cnefe.endereco_completo,
            filtered_cnefe.endereco_completo,
            filtered_cnefe.estado,
            filtered_cnefe.municipio,
            filtered_cnefe.logradouro,
            filtered_cnefe.numero,
            filtered_cnefe.cep,
            filtered_cnefe.localidade,
            filtered_cnefe.lat AS lat_cnefe,
            filtered_cnefe.lon AS lon_cnefe
      FROM filtered_cnefe
        WHERE lon BETWEEN {lon_min} AND {lon_max}
          AND lat BETWEEN {lat_min} AND {lat_max};"
      )

    cnefe_nearby <- DBI::dbGetQuery(con, query_get_nearby)

    cnefe_nearest <- cbind(temp_df, cnefe_nearby)

    # find the closest point
    if (any(!is.na(cnefe_nearest$lat_cnefe))) {
      cnefe_nearest[, distancia_metros := dt_haversine(lat,lon , lat_cnefe, lon_cnefe)]
      cnefe_nearest <- cnefe_nearest[cnefe_nearest[, .I[distancia_metros == min(distancia_metros)], by = tempidgeocodebr]$V1]
      cnefe_nearest <- cnefe_nearest[cnefe_nearest[, .I[1], by = tempidgeocodebr]$V1]
    }

    return(cnefe_nearest)
  }

  # apply function to all rows in the input table

  #if(n_cores==1){
  output <- pbapply::pblapply(
    X = 1:nrow(coords),
    FUN = reverse_geocode_single_row,
    coords = coords,
    con = con,
    dist_max = dist_max
  )


  output <- data.table::rbindlist(output, fill = TRUE)

  # # find the closest point
  # output[, distancia_metros := dt_haversine(lat,lon , lat_cnefe, lon_cnefe)]
  # output <- output[output[, .I[distancia_metros == min(distancia_metros)], by = tempidgeocodebr]$V1]
  # output <- output[output[, .I[1], by = tempidgeocodebr]$V1]

  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")
  duckdb::dbDisconnect(con)

  return(output)
}







reverse_geocode_filter_loop <- function(coordenadas,
                                   dist_max = 1000,
                                   verboso = TRUE,
                                   cache = TRUE,
                                   n_cores = 1){

  # check input
  checkmate::assert_logical(verboso)
  checkmate::assert_number(n_cores)
  checkmate::assert_number(dist_max, lower = 1000, finite = TRUE)
  checkmate::assert_logical(cache)
  checkmate::assert_class(coordenadas, 'sf')

  epsg <- sf::st_crs(coordenadas)$epsg
  if (epsg != 4674) { stop('Dados de input precisam estar com projeção geográfica SIRGAS 2000, EPSG 4674')}


  # prep input -------------------------------------------------------

  # converte para data.frame
  coords <- sfheaders::sf_to_df(coordenadas, fill = TRUE)
  data.table::setDT(coords)
  coords[, c('sfg_id', 'point_id') := NULL]
  data.table::setnames(coords, old = c('x', 'y'), new = c('lon', 'lat'))

  # create temp id
  coords[, tempidgeocodebr := 1:nrow(coords) ]

  # convert max_dist to degrees
  # 1 degree of latitude is always 111320 meters
  margin_lat <- dist_max / 111320

  # 1 degree of longitude is 111320 * cos(lat)
  coords[, c("lat_min", "lat_max") := .(lat - margin_lat, lat + margin_lat)]

  coords[, c("lon_min", "lon_max") := .(lon - dist_max / 111320 * cos(lat),
                                        lon + dist_max / 111320 * cos(lat))
         ]

  # get bounding box around input points
  # using a range of max dist around input points
  bbox_lat_min <- min(coords$lat_min)
  bbox_lat_max <- max(coords$lat_max)
  bbox_lon_min <- min(coords$lon_min)
  bbox_lon_max <- max(coords$lon_max)


  # check if input falls within Brazil
  bbox_brazil <- data.frame(
    xmin = -73.99044997,
    ymin = -33.75208127,
    xmax = -28.83594354,
    ymax =   5.27184108
  )

  error_msg <- 'Coordenadas de input localizadas fora do bounding box do Brasil.'
  if(bbox_lon_min < bbox_brazil$xmin |
     bbox_lon_max > bbox_brazil$xmax |
     bbox_lat_min < bbox_brazil$ymin |
     bbox_lat_max > bbox_brazil$ymax) { stop(error_msg) }


  # download cnefe  -------------------------------------------------------

  # downloading cnefe
  cnefe_dir <- download_cnefe(
    verboso = verboso,
    cache = cache
  )


  # limita escopo de busca aos estados  -------------------------------------------------------

  # determine potential states
  bbox_states <- readRDS(system.file("extdata/states_bbox.rds", package = "geocodebr"))
  potential_states <- dplyr::filter(
    bbox_states,
    xmin >= bbox_lon_min |
      xmax <= bbox_lon_max |
      ymin >= bbox_lat_min |
      ymax <= bbox_lat_max)$abbrev_state

  # Load CNEFE data and filter it to include only states
  # present in the input table, reducing the search scope
  # Narrow search global scope of cnefe to bounding box
  path_to_parquet <- paste0(listar_pasta_cache(), "/municipio_logradouro_numero_cep_localidade.parquet")
  filtered_cnefe <- arrow_open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% potential_states) |>
    dplyr::filter(lon >= bbox_lon_min &
                    lon <= bbox_lon_max &
                    lat >= bbox_lat_min &
                    lat <= bbox_lat_max) |>
    dplyr::compute()


  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)


  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_table_db", coords,
                       temporary = TRUE, overwrite = TRUE)

  # create output db
  query_create_empty_output_db <- glue::glue(
    "CREATE OR REPLACE TABLE output_db (
     tempidgeocodebr INTEGER,
     lat_cnefe NUMERIC(9, 7),
     lon_cnefe NUMERIC(9, 7),
     endereco_completo VARCHAR,
     estado VARCHAR,
     municipio VARCHAR,
     logradouro VARCHAR,
     numero VARCHAR,
     cep VARCHAR,
     localidade VARCHAR);"
    )

  DBI::dbExecute(con, query_create_empty_output_db)



  # START SEARCH -----------------------------------------------

  # start progress bar
  if (verboso) {
    prog <- create_progress_bar(coords)
    message_looking_for_matches()
  }

  n_rows <- nrow(coords)
  matched_rows <- 0

  # define raios de busca
  n_thresholds <- ifelse(dist_max < 5000, 2, 4)
  increments <- round(dist_max/n_thresholds)
  dist_thresholds <- c(seq(200, dist_max, increments), dist_max)


  # start matching
  for (dist in dist_thresholds ) {

    if (verboso) update_progress_bar(matched_rows, dist)


      n_rows_affected <- serch_nearby_addresses(
        con = con,
        dist = dist
      )

      # update progress bar
      matched_rows <- matched_rows + n_rows_affected

      # leave the loop early if we find all addresses before covering all cases
      if (matched_rows == n_rows) break
  }

  if (verboso) finish_progress_bar(matched_rows)



  # output with all original columns
  duckdb::dbWriteTable(con, "input_db", coords,
                       temporary = TRUE, overwrite=TRUE)


  query <- glue::glue(
    "SELECT *
      FROM input_db
      LEFT JOIN output_db
      ON input_db.tempidgeocodebr = output_db.tempidgeocodebr;"
    )

  # Execute the query and fetch the merged data
  output <- DBI::dbGetQuery(con, query)

  # organize output -------------------------------------------------

  data.table::setDT(output)
  output[, c('lon_min', 'lon_max', 'lat_min', 'lat_max') := NULL]

  # find the closest point
  output[, distancia_metros := dt_haversine(lat,lon , lat_cnefe, lon_cnefe)]
  output[is.na(distancia_metros), distancia_metros := Inf]
  output <- output[output[, .I[distancia_metros == min(distancia_metros)], by = tempidgeocodebr]$V1]
  output[, distancia_metros := data.table::fifelse(distancia_metros == Inf, NA, distancia_metros)]
  output[, distancia_metros := as.integer(distancia_metros)]
  output <- output[order(tempidgeocodebr)]
  # summary(output$distancia_metros)

  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")
  duckdb::dbDisconnect(con)

  return(output)
}
