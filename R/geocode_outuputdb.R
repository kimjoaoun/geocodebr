geocode_db <- function(enderecos,
                    campos_endereco = listar_campos(),
                    resultado_completo = FALSE,
                    verboso = TRUE,
                    cache = TRUE,
                    n_cores = 1
                    ){
  # check input
  checkmate::assert_data_frame(enderecos)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)
  checkmate::assert_logical(verboso, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  checkmate::assert_logical(resultado_completo, any.missing = FALSE, len = 1)
  campos_endereco <- assert_and_assign_address_fields(
    campos_endereco,
    enderecos
  )

  # normalize input data -------------------------------------------------------

  # standardizing the addresses table to increase the chances of finding a match
  # in the CNEFE data

  if (verboso) message_standardizing_addresses()

  input_padrao <- enderecobr::padronizar_enderecos(
    enderecos,
    campos_do_endereco = enderecobr::correspondencia_campos(
      logradouro = campos_endereco[["logradouro"]],
      numero = campos_endereco[["numero"]],
      cep = campos_endereco[["cep"]],
      bairro = campos_endereco[["localidade"]],
      municipio = campos_endereco[["municipio"]],
      estado = campos_endereco[["estado"]]
    ),
    formato_estados = "sigla",
    formato_numeros = 'integer'
  )


  # keep and rename colunms of input_padrao to use the
  # same column names used in cnefe data set
  data.table::setDT(input_padrao)
  cols_to_keep <- names(input_padrao)[! names(input_padrao) %in% campos_endereco]
  input_padrao <- input_padrao[, .SD, .SDcols = c(cols_to_keep)]
  names(input_padrao) <- c(gsub("_padr", "", names(input_padrao)))

  data.table::setnames(
    x = input_padrao,
    old = c('logradouro', 'bairro'),
    new = c('logradouro_sem_numero', 'localidade'))

  # create temp id
  input_padrao[, tempidgeocodebr := 1:nrow(input_padrao) ]
  data.table::setDT(enderecos)[, tempidgeocodebr := 1:nrow(input_padrao) ]

  # # sort input data
  # input_padrao <- input_padrao[order(estado, municipio, logradouro_sem_numero, numero, cep, localidade)]


  # downloading cnefe
  cnefe_dir <- download_cnefe(
    verboso = verboso,
    cache = cache
  )


  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                       overwrite = TRUE, temporary = TRUE)


  # create an empty output table that will be populated -----------------------------------------------

  additional_cols <- ""
  if (isTRUE(resultado_completo)) {
    additional_cols <- glue::glue(", endereco_encontrado VARCHAR")
    # additional_cols <- glue::glue(
    #   ", endereco_encontrado VARCHAR, logradouro_encontrado VARCHAR, ",
    #   "numero_encontrado VARCHAR, localidade_encontrada VARCHAR, ",
    #   "cep_encontrado VARCHAR, municipio_encontrado VARCHAR, estado_encontrado VARCHAR"
    # )
  }

  query_create_empty_output_db <- glue::glue(
    "CREATE TABLE output_db (
     tempidgeocodebr INTEGER,
     lat NUMERIC,
     lon NUMERIC,
     tipo_resultado VARCHAR {additional_cols});"
  )

  DBI::dbExecute(con, query_create_empty_output_db)

  # START MATCHING -----------------------------------------------

  # start progress bar
  if (verboso) {
    prog <- create_progress_bar(input_padrao)
    message_looking_for_matches()
  }

  n_rows <- nrow(input_padrao)
  matched_rows <- 0

  # start matching
  for (case in all_possible_match_types ) {

    relevant_cols <- get_relevant_cols_arrow(case)

    if (verboso) update_progress_bar(matched_rows, case)


    if (all(relevant_cols %in% names(input_padrao))) {

      # select match function
      match_fun <- ifelse(case %in% number_interpolation_types, match_weighted_cases2, match_cases2)

      n_rows_affected <- match_fun(
        con,
        x = 'input_padrao_db',
        y = 'filtered_cnefe', # keep this for now
        output_tb = "output_db",
        key_cols = relevant_cols,
        match_type = case,
        resultado_completo = resultado_completo
      )

      matched_rows <- matched_rows + n_rows_affected

      # leave the loop early if we find all addresses before covering all cases
      if (matched_rows == n_rows) break
    }

  }

  if (verboso) finish_progress_bar(matched_rows)


  # add precision column
  add_precision_col(con, update_tb = 'output_db')

  # output with all original columns
  duckdb::dbWriteTable(con, "input_db", enderecos,
                       temporary = TRUE, overwrite=TRUE)

  x_columns <- names(enderecos)

  output_deterministic <- merge_results(
    con,
    x='input_db',
    y='output_db',
    key_column='tempidgeocodebr',
    select_columns = x_columns,
    resultado_completo = resultado_completo
  )

  # Disconnect from DuckDB when done
  duckdb::dbDisconnect(con)

  # Return the result
  return(output_deterministic)
}


match_cases2 <- function(con,
                        x,
                        y,
                        output_tb,
                        key_cols,
                        match_type,
                        resultado_completo
                        ){

  # read correspondind parquet file
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio', 'municipio', table_name)
  table_name <- gsub('logradouro_sem_numero', 'logradouro', table_name)

  # build path to local file
  path_to_parquet <- paste0(listar_pasta_cache(), "/", table_name, ".parquet")

  # determine geographical scope of the search
  input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado
  input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio

  # Load CNEFE data and write to DuckDB
  # filter cnefe to include only states and municipalities
  # present in the input table, reducing the search scope
  filtered_cnefe <- arrow::open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()


  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)


  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("filtered_cnefe.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # whether to keep all columns in the result
  colunas_encontradas <- ""
  additional_cols <- ""

  if (isTRUE(resultado_completo)) {
    colunas_encontradas <- paste0(", endereco_encontrado")
    additional_cols <- paste0(", filtered_cnefe.endereco_completo AS endereco_encontrado")

    #
    #   colunas_encontradas <- glue::glue( ", endereco_encontrado, estado_encontrado,
    #       municipio_encontrado, logradouro_encontrado, numero_encontrado,
    #       cep_encontrado, localidade_encontrada"
    #     )
    #
    #   additional_cols <- paste0(
    #     glue::glue("filtered_cnefe.{key_cols} AS {key_cols}_encontrado"),
    #     collapse = ', ')
    #
    #   additional_cols <- gsub('logradouro_sem_numero_encontrado', 'logradouro_encontrado', additional_cols)
    #   additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    #   additional_cols <- paste0(", filtered_cnefe.endereco_completo AS endereco_encontrado, ", additional_cols)
  }

  # summarize query
  query_match <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado {colunas_encontradas})
      SELECT {x}.tempidgeocodebr, filtered_cnefe.lat, filtered_cnefe.lon,
             '{match_type}' AS tipo_resultado {additional_cols}
      FROM {x}
      LEFT JOIN filtered_cnefe
      ON {join_condition}
      WHERE {cols_not_null} AND filtered_cnefe.lon IS NOT NULL;"
  )

  temp_n <- DBI::dbExecute(con, query_match)


  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")

  # UPDATE input_padrao_db: Remove observations found in previous step
  update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
}



match_weighted_cases2 <- function(con,
                                 x,
                                 y,
                                 output_tb,
                                 key_cols,
                                 match_type,
                                 resultado_completo){


  # read correspondind parquet file
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio', 'municipio', table_name)
  table_name <- gsub('logradouro_sem_numero', 'logradouro_numero', table_name)

  # build path to local file
  path_to_parquet <- paste0(geocodebr::get_cache_dir(), "/", table_name, ".parquet")

  # determine geographical scope of the search
  input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado
  input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio

  # Load CNEFE data and write to DuckDB
  # filter cnefe to include only states and municipalities
  # present in the input table, reducing the search scope
  filtered_cnefe <- arrow::open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)


  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # whether to keep all columns in the result
  colunas_encontradas <- ""
  additional_cols <- ""

  if (isTRUE(resultado_completo)) {
    colunas_encontradas <- paste0(", endereco_encontrado")
    additional_cols <- paste0(", filtered_cnefe.endereco_completo AS endereco_encontrado")

    #
    #   colunas_encontradas <- glue::glue( ", endereco_encontrado, estado_encontrado,
    #       municipio_encontrado, logradouro_encontrado, numero_encontrado,
    #       cep_encontrado, localidade_encontrada"
    #     )
    #
    #   additional_cols <- paste0(
    #     glue::glue("filtered_cnefe.{key_cols} AS {key_cols}_encontrado"),
    #     collapse = ', ')
    #
    #   additional_cols <- gsub('logradouro_sem_numero_encontrado', 'logradouro_encontrado', additional_cols)
    #   additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    #   additional_cols <- paste0(", filtered_cnefe.endereco_completo AS endereco_encontrado, ", additional_cols)
  }

  # match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS ",
    "SELECT {x}.tempidgeocodebr, {x}.numero, {y}.numero AS numero_db, ",
    "{y}.lat, {y}.lon {additional_cols} ",
    "FROM {x} ",
    "LEFT JOIN {y} ",
    "ON {join_condition} ",
    "WHERE {cols_not_null} AND {x}.numero IS NOT NULL AND {y}.numero IS NOT NULL;"
  )

  DBI::dbExecute(con, query_match)


  # summarize query
  query_aggregate <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado)
      SELECT tempidgeocodebr,
      SUM((1/ABS(numero - numero_db) * lat)) / SUM(1/ABS(numero - numero_db)) AS lat,
      SUM((1/ABS(numero - numero_db) * lon)) / SUM(1/ABS(numero - numero_db)) AS lon,
      '{match_type}' AS tipo_resultado,
      FROM temp_db
      GROUP BY tempidgeocodebr;"
  )


  if (isTRUE(resultado_completo)) {

    query_aggregate <- glue::glue(
      "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado {colunas_encontradas})
        SELECT tempidgeocodebr,
        SUM((1/ABS(numero - numero_db) * lat)) / SUM(1/ABS(numero - numero_db)) AS lat,
        SUM((1/ABS(numero - numero_db) * lon)) / SUM(1/ABS(numero - numero_db)) AS lon,
        '{match_type}' AS tipo_resultado,
         REGEXP_REPLACE(FIRST(endereco_completo), ', \\d+ -', CONCAT(', ', FIRST(numero), ' (aprox) -')) AS endereco_encontrado
      FROM temp_db
      GROUP BY tempidgeocodebr;"
    )

  }

  temp_n <- DBI::dbExecute(con, query_match)


  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")

  # UPDATE input_padrao_db: Remove observations found in previous step
  update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )



  return(temp_n)
}

