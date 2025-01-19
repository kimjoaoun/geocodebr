geocode_dani_arrow <- function(addresses_table,
                         address_fields = setup_address_fields(),
                         n_cores = 1,
                         progress = TRUE,
                         full_results = FALSE,
                         cache = TRUE) {

  # check input
  assert_address_fields(address_fields, addresses_table)
  checkmate::assert_data_frame(addresses_table)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)
  checkmate::assert_logical(progress, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  checkmate::assert_logical(full_results, any.missing = FALSE, len = 1)

  # standardizing the addresses table to increase the chances of finding a match
  # in the CNEFE data

  if (progress) message_standardizing_addresses()

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
    formato_estados = "sigla",
    formato_numeros = 'integer'
  )

  # create temp id
  standard_locations[, tempidgeocodebr := 1:nrow(standard_locations) ]


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

  if( isTRUE(full_results)){
    DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN matched_address VARCHAR DEFAULT NULL")
  }

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

  input_municipio <- input_municipio[!is.na(input_municipio)]
  if(is.null(input_municipio)){ input_municipio <- "*"}


  # start progress bar
  if (progress) {
    prog <- create_progress_bar(standard_locations)
    message_looking_for_matches()
  }

  n_rows <- nrow(standard_locations)
  matched_rows <- 0


  # start matching
  for (case in all_possible_match_types) {
    relevant_cols <- get_relevant_cols_dani_arrow(case)

    if (progress) update_progress_bar(matched_rows, case)

    if (all(relevant_cols %in% names(standard_locations))) {

      # select match function
      match_fun <- ifelse(case %in% number_interpolation_types, lookup_weighted_cases, lookup_cases)

      n_rows_affected <- match_fun(
        con,
        relevant_cols = relevant_cols,
        case = case,
        full_results = full_results,
        lookup_vector = lookup_vector,
        input_states = input_states,
        input_municipio = input_municipio
      )

      matched_rows <- matched_rows + n_rows_affected

      # leave the loop early if we find all addresses before covering all cases
      if (matched_rows == n_rows) break
    }
  }

  if (progress) finish_progress_bar(matched_rows)

  # add precision column
  add_precision_col(con, update_tb = 'standard_locations')

  # select cols to keep in the output
  cols_to_keep <- names(standard_locations)
  cols_to_keep <- cols_to_keep[!grepl("_padr$", cols_to_keep)]
  cols_to_keep <- cols_to_keep[cols_to_keep != "tempidgeocodebr"]
  cols_to_keep <- c(cols_to_keep, "precision", "match_type",  "lat", "lon")
    if(isTRUE(full_results)){ cols_to_keep <- c(cols_to_keep, "matched_address") }

  cols_to_keep <- paste(cols_to_keep, collapse = ", ")

  query_output <- glue::glue(
    "SELECT {cols_to_keep} FROM standard_locations"
    )

  output <-  DBI::dbGetQuery(con, query_output)


  # Disconnect from DuckDB when done
  duckdb::dbDisconnect(con)


  return(output)
}


