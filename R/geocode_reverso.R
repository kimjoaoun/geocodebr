#' Geocode reverso de coordenadas espaciais no Brasil
#'
#' @description
#' Geocode reverso de coordenadas geogr<c3><a1>ficas para endere<c3><a7>os. A
#' fun<c3><a7><c3><a3>o recebe um `sf data frame` com pontos e retorna o
#' endere<c3><a7>o mais pr<c3><b3>ximo dando uma dist<c3><a2>ncia m<c3><a1>xima
#' de busca.
#'
#' @param pontos Uma tabela de dados com classe espacial `sf data frame`.
#' @param dist_max Integer. Distancia m<c3><a1>xima aceit<c3><a1>vel (em metros)
#'        entre os pontos de input e o endere<c3><a7>o Por padr<c3><a3>o, a
#'        dist<c3><a2>ncia <c3><a9> de 1000 metros.
#' @template verboso
#' @template cache
#' @template n_cores
#'
#' @return Retorna o `sf data.frame` de input adicionado das colunas do
#'         endere<c3><a7>o a dist<c3><a2>ncia entre o ponto de input e o
#'         endere<c3><a7>o mais pr<c3><b3>ximo.
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' library(geocodebr)
#' library(sf)
#'
#' # ler amostra de dados
#' pontos <- readRDS(
#'     system.file("extdata/pontos.rds", package = "geocodebr")
#'     )
#'
#' pontos <- pontos[1:50,]
#'
#' # geocode reverso
#' df_enderecos <- geocodebr::geocode_reverso(
#'   pontos = pontos,
#'   dist_max = 1000,
#'   verboso = TRUE,
#'   n_cores = 1
#'   )
#'
#' @export
geocode_reverso <- function(pontos,
                            dist_max = 1000,
                            verboso = TRUE,
                            cache = TRUE,
                            n_cores = 1){

  # check input
  checkmate::assert_class(pontos, 'sf')
  checkmate::assert_number(dist_max, lower = 500, upper = 100000) # max 100 Km
  checkmate::assert_logical(verboso)
  checkmate::assert_logical(cache)
  checkmate::assert_number(n_cores)

  epsg <- sf::st_crs(pontos)$epsg
  if (epsg != 4674) {
    cli::cli_abort("Dados de input precisam estar com proje<c3><a7><c3><a3>o geogr<c3><a1>fica SIRGAS 2000, EPSG 4674")
  }


  # prep input -------------------------------------------------------

  # converte para data.frame
  coords <- sfheaders::sf_to_df(pontos, fill = TRUE)
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

  # convert df to simple feature
  output_sf <- sfheaders::sf_point(
      obj = output,
      x = 'lon',
      y = 'lat',
      keep = TRUE
    )

  sf::st_crs(output_sf) <- 4674

  return(output_sf)
}
