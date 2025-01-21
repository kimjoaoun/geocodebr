match_cases <- function(con,
                        x,
                        y,
                        output_tb,
                        key_cols,
                        match_type,
                        full_results){

  # read correspondind parquet file
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio', 'municipio', table_name)
  table_name <- gsub('logradouro_sem_numero', 'logradouro', table_name)

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
    glue::glue("filtered_cnefe.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  cols_not_null <-  paste(
    glue::glue("filtered_cnefe.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )


  # whether to keep all columns in the result

  colunas_encontradas <- ""
  additional_cols <- ""

  if (isTRUE(full_results)) {
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


  # query for left join
  query_match <- glue::glue(
    "CREATE TEMPORARY TABLE {output_tb} AS
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
