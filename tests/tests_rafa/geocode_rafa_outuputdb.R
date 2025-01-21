geocode_db <- function(addresses_table,
                    address_fields = setup_address_fields(),
                    n_cores = 1,
                    progress = TRUE,
                    keep_matched_address = FALSE,
                    cache = TRUE
                    ){
  # check input
  assert_address_fields(address_fields, addresses_table)
  checkmate::assert_data_frame(addresses_table)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)
  checkmate::assert_logical(progress, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  checkmate::assert_logical(keep_matched_address, any.missing = FALSE, len = 1)

  # normalize input data -------------------------------------------------------

  # standardizing the addresses table to increase the chances of finding a match
  # in the CNEFE data

  if (progress) message_standardizing_addresses()

  input_padrao <- enderecobr::padronizar_enderecos(
    addresses_table,
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


  # keep and rename colunms of input_padrao to use the
  # same column names used in cnefe data set
  data.table::setDT(input_padrao)
  cols_to_keep <- names(input_padrao)[! names(input_padrao) %in% address_fields]
  input_padrao <- input_padrao[, .SD, .SDcols = c(cols_to_keep)]
  names(input_padrao) <- c(gsub("_padr", "", names(input_padrao)))

  data.table::setnames(
    x = input_padrao,
    old = c('logradouro', 'bairro'),
    new = c('logradouro_sem_numero', 'localidade'))

  # create temp id
  input_padrao[, tempidgeocodebr := 1:nrow(input_padrao) ]
  data.table::setDT(addresses_table)[, tempidgeocodebr := 1:nrow(input_padrao) ]

  # # sort input data
  # input_padrao <- input_padrao[order(estado, municipio, logradouro_sem_numero, numero, cep, localidade)]


  # downloading cnefe
  cnefe_dir <- download_cnefe(
    progress = progress,
    cache = cache
  )


  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                       overwrite = TRUE, temporary = TRUE)


  # create an empty output table that will be populated -----------------------------------------------

  query_create_empty_output_db <- glue::glue(
    "CREATE OR REPLACE TABLE output_db (
    tempidgeocodebr INTEGER,
    lon NUMERIC,
    lat NUMERIC,
    match_type VARCHAR, matched_address VARCHAR);"
  )

  if (isFALSE(keep_matched_address)) {
    query_create_empty_output_db <- gsub(", matched_address VARCHAR);", ");",
                                         query_create_empty_output_db)
  }

  DBI::dbExecute(con, query_create_empty_output_db)

  # START MATCHING -----------------------------------------------

  # determine geographical scope of the search
  input_states <- unique(input_padrao$estado)
  input_municipio <- unique(input_padrao$municipio)

  input_municipio <- input_municipio[!is.na(input_municipio)]
  if(is.null(input_municipio)){ input_municipio <- "*"}

  # start progress bar
  if (progress) {
    prog <- create_progress_bar(input_padrao)

    message_looking_for_matches()
  }

  n_rows <- nrow(input_padrao)
  matched_rows <- 0

  # start matching
  for (case in all_possible_match_types ) {
    relevant_cols <- get_relevant_cols_arrow(case)

    if (progress) update_progress_bar(matched_rows, case)


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
        keep_matched_address = keep_matched_address,
        input_states = input_states,
        input_municipio = input_municipio
      )

      matched_rows <- matched_rows + n_rows_affected

      # leave the loop early if we find all addresses before covering all cases
      if (matched_rows == n_rows) break
    }

  }

  if (progress) finish_progress_bar(matched_rows)


  # prepare output -----------------------------------------------
  # # THIS could BE IMPROVED / optimized
  #
  # # list all table outputs
  # all_possible_tables <- glue::glue("output_{all_possible_match_types}")
  #
  # # check which tables have been created
  # output_tables <- lapply(
  #   X= all_possible_tables,
  #   FUN = function(i){ ifelse( DBI::dbExistsTable(con, i), i, 'empty') }) |>
  #   unlist()
  #
  # all_output_tbs <- output_tables[!grepl('empty', output_tables)]
  #
  # # save output to db
  # output_query <- paste("CREATE TEMPORARY TABLE output_db AS",
  #                       paste0("SELECT ", paste0('*', " FROM ", all_output_tbs),
  #                              collapse = " UNION ALL ")
  #                       )
  #
  # DBI::dbExecute(con, output_query)

  # add precision column
  add_precision_col(con, update_tb = 'output_db')

  # output with all original columns
  duckdb::dbWriteTable(con, "input_db", addresses_table,
                       temporary = TRUE, overwrite=TRUE)

  x_columns <- names(addresses_table)

  output_deterministic <- merge_results(
    con,
    x='input_db',
    y='output_db',
    key_column='tempidgeocodebr',
    select_columns = x_columns,
    keep_matched_address = keep_matched_address
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
                        keep_matched_address,
                        input_states,
                        input_municipio
){

  # read correspondind parquet file
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio', 'municipio', table_name)
  table_name <- gsub('logradouro_sem_numero', 'logradouro', table_name)
  y <- table_name

  # build path to local file
  path_to_parquet <- paste0(geocodebr::get_cache_dir(), "/", table_name, ".parquet")

  # filter cnefe to include only states and municipalities
  # present in the input table, reducing the search scope and consequently
  # reducing processing time and memory usage

  # Load CNEFE data and write to DuckDB
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

  # # TO DO: match probabilistico
  # # isso eh um teste provisorio
  # if( match_type %in% probabilistic_logradouro_match_types) {
  #   join_condition <- gsub("= input_padrao_db.logradouro_sem_numero", "LIKE '%' || input_padrao_db.logradouro_sem_numero || '%'", join_condition)
  #   }

  # query for left join
  query_match <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lon, lat, match_type, matched_address)
      SELECT {x}.tempidgeocodebr, filtered_cnefe.lon, filtered_cnefe.lat, '{match_type}' AS match_type, filtered_cnefe.endereco_completo AS matched_address
      FROM {x}
      LEFT JOIN filtered_cnefe
      ON {join_condition}
      WHERE {x}.numero IS NOT NULL AND filtered_cnefe.lon IS NOT NULL;"
  )

  if (match_type %in% possible_match_types_no_number) {
    query_match <- gsub("input_padrao_db.numero IS NOT NULL AND", "", query_match)
  }

  if (isFALSE(keep_matched_address)) {
    query_match <- gsub("lat, match_type, matched_address)", "lat, match_type)", query_match)
  }

  if (isFALSE(keep_matched_address)) {
    query_match <- gsub(", filtered_cnefe.endereco_completo AS matched_address", "", query_match)
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



match_weighted_cases2 <- function(con,
                                 x,
                                 y,
                                 output_tb,
                                 key_cols,
                                 match_type,
                                 keep_matched_address,
                                 input_states,
                                 input_municipio){


  # read correspondind parquet file
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio', 'municipio', table_name)
  table_name <- gsub('logradouro_sem_numero', 'logradouro_numero', table_name)
  y <- table_name

  # build path to local file
  path_to_parquet <- paste0(geocodebr::get_cache_dir(), "/", table_name, ".parquet")

  # filter cnefe to include only states and municipalities
  # present in the input table, reducing the search scope and consequently
  # reducing processing time and memory usage

  # Load CNEFE data and write to DuckDB
  filtered_cnefe <- arrow::open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)

  y <- 'filtered_cnefe'

  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  # # TO DO: match probabilistico
  # # isso eh um teste provisorio
  # if( match_type %in% probabilistic_logradouro_match_types) {
  #   join_condition <- gsub("= input_padrao_db.logradouro_sem_numero", "LIKE '%' || input_padrao_db.logradouro_sem_numero || '%'", join_condition)
  #   }

  # Construct the SQL match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS
      SELECT {x}.tempidgeocodebr, {x}.numero, {y}.numero as numero_db, {y}.lat, {y}.lon, {y}.endereco_completo
      FROM {x}
      LEFT JOIN {y}
      ON {join_condition}
      WHERE {x}.numero IS NOT NULL AND {y}.numero IS NOT NULL;"
  )

  if (isFALSE(keep_matched_address)) {
    query_match <- gsub(", filtered_cnefe.endereco_completo", "", query_match)
  }

  DBI::dbExecute(con, query_match)


  # summarize
  query_aggregate <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lon, lat, match_type, matched_address)
      SELECT tempidgeocodebr,
      SUM((1/ABS(numero - numero_db) * lon)) / SUM(1/ABS(numero - numero_db)) AS lon,
      SUM((1/ABS(numero - numero_db) * lat)) / SUM(1/ABS(numero - numero_db)) AS lat,
      '{match_type}' AS match_type,
       REGEXP_REPLACE(FIRST(endereco_completo), ', \\d+ -', CONCAT(', ', FIRST(numero), ' (aprox) -')) AS matched_address
      FROM temp_db
      GROUP BY tempidgeocodebr;"
  )

  if (isFALSE(keep_matched_address)) {
    query_aggregate <- glue::glue(
      "INSERT INTO output_db (tempidgeocodebr, lon, lat, match_type)
          SELECT tempidgeocodebr,
          SUM((1/ABS(numero - numero_db) * lon)) / SUM(1/ABS(numero - numero_db)) AS lon,
          SUM((1/ABS(numero - numero_db) * lat)) / SUM(1/ABS(numero - numero_db)) AS lat,
          '{match_type}' AS match_type
          FROM temp_db
          GROUP BY tempidgeocodebr;"
    )
  }

  temp_n <- DBI::dbExecute(con, query_aggregate)
  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")


  # UPDATE input_padrao_db: Remove observations found in previous step
  update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )



  return(temp_n)
}

