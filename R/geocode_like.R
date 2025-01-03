#' Geocode Brazilian addresses
#'
#' Geocodes Brazilian addresses based on CNEFE data. Addresses must be passed as
#' a data frame in which each column describes one address field (street name,
#' street number, neighborhood, etc). The input addresses are matched with CNEFE
#' following 12 different case patterns. For more info, please see the Details
#' section.
#'
#' @param addresses_table A data frame. The addresses to be geocoded. Each
#'   column must represent an address field.
#' @param address_fields A character vector. The correspondence between each
#'   address field and the name of the column that describes it in
#'   `addresses_table`. The [setup_address_fields()] function helps creating
#'   this vector and performs some checks on the input. Address fields
#'   passed as `NULL` are ignored and the function must receive at least one
#'   non-null field. If manually creating the vector, please note that the
#'   vector names should be the same names used in the [setup_address_fields()]
#'   parameters.
#' @template n_cores
#' @template progress
#' @template cache
#'
#' @return Returns the data frame passed in `addresses_table` with the latitude
#'   (`lat`) and longitude (`lon`) of each matched address, as well as another
#'   column (`match_type`) indicating the match level with which the address was
#'   matched.
#'
#' @details
#' The input addresses are deterministically matched with CNEFE following 12
#' different case patterns. The type of match found for each address in the
#' input data is indicated by the `match_type` column in the output. In every
#' match type, the function always calculates the average latitude and longitude
#' of all addresses in CNEFE that match the input address. In the strictest case,
#' the function finds a perfect match for all of the fields of a given address.
#' Think for example of a building with several apartments that match the same
#' street address. In such case, the coordinates of the apartments will differ
#' very slightly, and {geocodebr} take the average of those coordinates. On the
#' other hand, in the loosest case, in which only the state and the city are
#' matched, geocodebr takes the city-wide average coordinates, which tends to
#' favor more densely populated areas. The columns considered in each of the 12
#' different match types are described below:
#'
#' - Case 01: estado, município, logradouro, número, cep e bairro;
#' - Case 02: estado, município, logradouro, número e cep;
#' - Case 03: estado, município, logradouro, número e bairro;
#' - Case 04: estado, município, logradouro e número;
#' - Case 05: estado, município, logradouro, cep e bairro;
#' - Case 06: estado, município, logradouro e cep;
#' - Case 07: estado, município, logradouro e bairro;
#' - Case 08: estado, município e logradouro;
#' - Case 09: estado, município, cep e bairro;
#' - Case 10: estado, município e cep;
#' - Case 11: estado, município e bairro;
#' - Case 12: estado, município.
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#'
#' data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
#' input_df <- read.csv(data_path)
#'
#' fields <- setup_address_fields(
#'   logradouro = "nm_logradouro",
#'   numero = "Numero",
#'   cep = "Cep",
#'   bairro = "Bairro",
#'   municipio = "nm_municipio",
#'   estado = "nm_uf"
#' )
#'
#' df <- geocode(input_df, address_fields = fields, progress = FALSE)
#' df
#'
geocode_like <- function(addresses_table,
                    address_fields = setup_address_fields(),
                    n_cores = 1,
                    progress = TRUE,
                    cache = TRUE) {

  # check input
  assert_address_fields(address_fields, addresses_table)
  checkmate::assert_data_frame(addresses_table)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)
  checkmate::assert_logical(progress, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)

  # standardizing the addresses table to increase the chances of finding a match
  # in the CNEFE data

  if (progress) message_standardizing_addresses()

  standard_locations <- enderecobr::padronizar_enderecos(
    addresses_table,
    campos_do_endereco = enderecobr::correspondencia_campos(
      logradouro = address_fields[["logradouro"]],
      numero = address_fields[["numero"]],
      cep = address_fields[["cep"]],
      bairro = address_fields[["bairro"]],
      municipio = address_fields[["municipio"]],
      estado = address_fields[["estado"]]
    ),
    formato_estados = "sigla"
  )

  # downloading cnefe. we only need to download the states present in the
  # addresses table, which may save us some time.

  present_states <- unique(standard_locations$estado_padr)
  cnefe_dir <- download_cnefe(
    present_states,
    progress = progress,
    cache = cache
  )

  # creating a temporary db and register the input table data

  tmpdb <- tempfile(fileext = ".duckdb")
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = tmpdb)

  DBI::dbExecute(con, glue::glue("SET threads = {n_cores}"))

  duckdb::dbWriteTable(
    con,
    name = "standard_locations",
    value = standard_locations,
    temporary = TRUE
  )

  # register cnefe data to db, but only include states and municipalities
  # present in the input table, reducing the search scope and consequently
  # reducing processing time and memory usage

  unique_muns <- unique(standard_locations$municipio_padr)

  filtered_cnefe <- arrow::open_dataset(cnefe_dir)
  filtered_cnefe <- dplyr::filter(
    filtered_cnefe,
    estado %in% present_states & municipio %in% unique_muns
  )

  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)

  # to find the coordinates of the addresses, we merge the input table with the
  # cnefe data. the column names used in the input table are different than the
  # ones used in cnefe, so we create a helper object to "translate" the column
  # names between datasets

  equivalent_colnames <- tibble::tribble(
    ~standard_locations, ~cnefe,
    "logradouro_padr",   "logradouro_sem_numero",
    "numero_padr",       "numero",
    "cep_padr",          "cep",
    "bairro_padr",       "localidade",
    "municipio_padr",    "municipio",
    "estado_padr",       "estado"
  )

  lookup_vector <- equivalent_colnames$cnefe
  names(lookup_vector) <- equivalent_colnames$standard_locations

  # when merging the data, we have several different cases with different confidence
  # levels. from best to worst, they are:
  #
  # - case 01: match estado, municipio, logradouro, numero, cep, localidade
  # - case 02: match estado, municipio, logradouro, numero, cep
  # - case 03: match estado, municipio, logradouro, numero, localidade
  # - case 04: match estado, municipio, logradouro, numero
  # - case 05: match estado, municipio, logradouro, cep, localidade
  # - case 06: match estado, municipio, logradouro, cep
  # - case 07: match estado, municipio, logradouro, localidade
  # - case 08: match estado, municipio, logradouro
  # - case 09: match estado, municipio, cep, localidade
  # - case 10: match estado, municipio, cep
  # - case 11: match estado, municipio, localidade
  # - case 12: match estado, municipio

  DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN match_type VARCHAR DEFAULT NULL")
  DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN lon DOUBLE DEFAULT NULL")
  DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN lat DOUBLE DEFAULT NULL")

  if (progress) {
    prog <- create_progress_bar(standard_locations)
    n_rows_affected <- 0

    message_looking_for_matches()
  }

  for (case in 1:12) {
    relevant_cols <- get_relevant_cols(case)
    formatted_case <- formatC(case, width = 2, flag = "0")

    if (progress) update_progress_bar(n_rows_affected, formatted_case)

    if (all(relevant_cols %in% names(standard_locations))) {
      join_condition <- paste(
        glue::glue("aggregated_cnefe.{lookup_vector[relevant_cols]} = standard_locations.{relevant_cols}"),
        collapse = " AND "
      )

      # use LIKE for logradouro
      join_condition <- gsub("= standard_locations.logradouro_padr", "LIKE '%' || standard_locations.logradouro_padr || '%'", join_condition)




      cnefe_cols <- paste(lookup_vector[relevant_cols], collapse = ", ")

      n_rows_affected <- DBI::dbExecute(
        con,
        glue::glue(
          "UPDATE standard_locations ",
          "SET lat = aggregated_cnefe.lat, lon = aggregated_cnefe.lon, match_type = 'case_{formatted_case}' ",
          "FROM ",
          "  (SELECT {cnefe_cols}, AVG(lon) AS lon, AVG(lat) AS lat FROM filtered_cnefe GROUP BY {cnefe_cols}) AS aggregated_cnefe ",
          "WHERE match_type IS NULL AND {join_condition}"
        )
      )
    }
  }

  if (progress) finish_progress_bar(n_rows_affected)

  cols_to_keep <- names(standard_locations)
  cols_to_keep <- cols_to_keep[!grepl("_padr$", cols_to_keep)]
  cols_to_keep <- c(cols_to_keep, "match_type", "lon", "lat")
  cols_to_keep <- paste(cols_to_keep, collapse = ", ")

  output <- dplyr::tbl(
    con,
    dplyr::sql(glue::glue("SELECT {cols_to_keep} FROM standard_locations"))
  )
  output <- dplyr::collect(output)

  duckdb::dbDisconnect(con, shutdown = TRUE)

  return(output)
}

assert_address_fields <- function(address_fields, addresses_table) {
  col <- checkmate::makeAssertCollection()
  checkmate::assert_names(
    names(address_fields),
    type = "unique",
    subset.of = c(
      "logradouro",
      "numero",
      "cep",
      "bairro",
      "municipio",
      "estado"
    ),
    add = col
  )
  checkmate::assert_names(
    address_fields,
    subset.of = names(addresses_table),
    add = col
  )
  checkmate::reportAssertions(col)

  return(invisible(TRUE))
}

message_standardizing_addresses <- function() {
  geocodebr_message(c("i" = "Standardizing input addresses"))
}

message_looking_for_matches <- function() {
  geocodebr_message(c("i" = "Looking for matches in CNEFE"))
}

get_relevant_cols <- function(case) {
  relevant_cols <- if (case == 1) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr", "cep_padr", "bairro_padr")
  } else if (case == 2) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr", "cep_padr")
  } else if (case == 3) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr", "bairro_padr")
  } else if (case == 4) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr")
  } else if (case == 5) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "cep_padr", "bairro_padr")
  } else if (case == 6) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "cep_padr")
  } else if (case == 7) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "bairro_padr")
  } else if (case == 8) {
    c("estado_padr", "municipio_padr", "logradouro_padr")
  } else if (case == 9) {
    c("estado_padr", "municipio_padr", "cep_padr", "bairro_padr")
  } else if (case == 10) {
    c("estado_padr", "municipio_padr", "cep_padr")
  } else if (case == 11) {
    c("estado_padr", "municipio_padr", "bairro_padr")
  } else if (case == 12) {
    c("estado_padr", "municipio_padr")
  }

  return(relevant_cols)
}

create_progress_bar <- function(standard_locations, .envir = parent.frame()) {
  cli::cli_progress_bar(
    total = nrow(standard_locations),
    format = "Matched addresses: {formatC(cli::pb_current, big.mark = ',', format = 'd')}/{formatC(cli::pb_total, big.mark = ',', format = 'd')} {cli::pb_bar} {cli::pb_percent} - {cli::pb_status}",
    clear = FALSE,
    .envir = .envir
  )
}

update_progress_bar <- function(n_rows_affected,
                                formatted_case,
                                .envir = parent.frame()) {
  cli::cli_progress_update(
    inc = n_rows_affected,
    status = glue::glue("Looking for case {formatted_case} matches"),
    force = TRUE,
    .envir = .envir
  )
}

finish_progress_bar <- function(n_rows_affected, .envir = parent.frame()) {
  cli::cli_progress_update(
    inc = n_rows_affected,
    status = "Done!",
    force = TRUE,
    .envir = .envir
  )
}
