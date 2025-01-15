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
#'   address field and the name of the column that describes it in the
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
#' # df <- geocodebr:::geocode_dani(input_df, address_fields = fields, progress = FALSE)
#' # df
#'
#' @export
geocode_dani_arrow <- function(addresses_table,
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

  # TEMP. necessario para garantir que numero de input 0 vire 'S/N'
  data.table::setDT(addresses_table)
  addresses_table[, address_fields['numero'] := as.character( get(address_fields['numero']) )]


  standard_locations <- enderecobr::padronizar_enderecos(
    enderecos = addresses_table,
    campos_do_endereco = enderecobr::correspondencia_campos(
      logradouro = address_fields[["logradouro"]],
      numero = address_fields[["numero"]],
      cep = address_fields[["cep"]],
      bairro = address_fields[["bairro"]],
      municipio = address_fields[["municipio"]],
      estado = address_fields[["estado"]]
    ),
    formato_estados = "sigla"
    #, formato_numeros = 'integer'
  )

  # create temp id
  standard_locations[, tempidgeocodebr := 1:nrow(standard_locations) ]

  ### convert "numero" to numeric
  standard_locations[numero_padr == "S/N", numero_padr := NA_integer_]
  standard_locations[, numero_padr := as.integer(numero_padr)]


  # downloading cnefe. we only need to download the states present in the
  # addresses table, which may save us some time.

  cnefe_dir <- download_cnefe(
    progress = progress,
    cache = cache
  )

  # creating a temporary db and register the input table data

  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)


  duckdb::dbWriteTable(
    con,
    name = "standard_locations",
    value = standard_locations,
    temporary = TRUE,
    overwrite = TRUE
    )


  DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN lat DOUBLE DEFAULT NULL")
  DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN lon DOUBLE DEFAULT NULL")
  DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN match_type VARCHAR DEFAULT NULL")
  DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN matched_address VARCHAR DEFAULT NULL")

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


  # START MATCHING -----------------------------------------------

  # determine geographical scope of the search
  input_states <- unique(standard_locations$estado_padr)
  input_municipio <- unique(standard_locations$municipio_padr)


  if (progress) {
    prog <- create_progress_bar(standard_locations)
    n_rows_affected <- 0

    message_looking_for_matches()
  }

  for (case in all_possible_match_types) {

    relevant_cols <- get_relevant_cols_dani_arrow(case)

    if (progress) update_progress_bar(n_rows_affected, case)

    if (all(relevant_cols %in% names(standard_locations))) {

      # select match function
      match_fun <- ifelse(case %in% number_interpolation_types, lookup_weighted_cases, lookup_cases)

      n_rows_affected <- match_fun(
        con,
        relevant_cols = relevant_cols,
        case = case,
        lookup_vector = lookup_vector,
        input_states = input_states,
        input_municipio = input_municipio
      )
    }
  }

  if (progress) finish_progress_bar(n_rows_affected)

  # add precision column
  add_precision_col(con, update_tb = 'standard_locations')


  cols_to_keep <- names(standard_locations)
  cols_to_keep <- cols_to_keep[!grepl("_padr$", cols_to_keep)]
  cols_to_keep <- cols_to_keep[cols_to_keep != "tempidgeocodebr"]
  cols_to_keep <- c(cols_to_keep, "precision", "match_type",  "lat", "lon", "matched_address")
  cols_to_keep <- paste(cols_to_keep, collapse = ", ")

  query_output <- glue::glue(
    "SELECT {cols_to_keep} FROM standard_locations"
    )

  output <-  DBI::dbGetQuery(con, query_output)


  # Disconnect from DuckDB when done
  duckdb::dbDisconnect(con)


  return(output)
}


