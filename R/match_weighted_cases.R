match_weighted_cases <- function(con,
                                  x,
                                  y,
                                  output_tb,
                                  key_cols,
                                  match_type,
                                  resultado_completo){

  # read corresponding parquet file
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio', 'municipio', table_name)
  table_name <- gsub('logradouro_sem_numero', 'logradouro', table_name)

  # build path to local file
  path_to_parquet <- paste0(geocodebr::listar_pasta_cache(), "/", table_name, ".parquet")

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

  # remove numero from key cols to allow for the matching
  key_cols <- key_cols[key_cols != 'numero']

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

    colunas_encontradas <- paste0(
      glue::glue("{key_cols}_encontrado"),
      collapse = ', ')

    colunas_encontradas <- gsub('logradouro_sem_numero_encontrado', 'logradouro_encontrado', colunas_encontradas)
    colunas_encontradas <- gsub('localidade_encontrado', 'localidade_encontrada', colunas_encontradas)
    colunas_encontradas <- paste0(", endereco_encontrado, ", colunas_encontradas)

    additional_cols <- paste0(
      glue::glue("filtered_cnefe.{key_cols} AS {key_cols}_encontrado"),
      collapse = ', ')

    additional_cols <- gsub('logradouro_sem_numero_encontrado', 'logradouro_encontrado', additional_cols)
    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", filtered_cnefe.endereco_completo AS endereco_encontrado, ", additional_cols)

  }

  # match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS
    SELECT {x}.tempidgeocodebr, {x}.numero, {y}.numero AS numero_db,
    {y}.lat, {y}.lon {additional_cols}
    FROM {x}
    LEFT JOIN {y}
    ON {join_condition}
    WHERE {cols_not_null} AND {x}.numero IS NOT NULL AND {y}.numero IS NOT NULL;"
  )

  DBI::dbExecute(con, query_match)


  # summarize query
  query_aggregate <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado)
      SELECT tempidgeocodebr,
      SUM((1/ABS(numero - numero_db) * lat)) / SUM(1/ABS(numero - numero_db)) AS lat,
      SUM((1/ABS(numero - numero_db) * lon)) / SUM(1/ABS(numero - numero_db)) AS lon,
      '{match_type}' AS tipo_resultado
      FROM temp_db
      GROUP BY tempidgeocodebr;"
  )


  if (isTRUE(resultado_completo)) {

    additional_cols <- paste0(
      glue::glue("FIRST({key_cols}_encontrado)"),
      collapse = ', ')

    additional_cols <- gsub('logradouro_sem_numero_encontrado', 'logradouro_encontrado', additional_cols)
    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)

    query_aggregate <- glue::glue(
      "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado {colunas_encontradas})
        SELECT tempidgeocodebr,
        SUM((1/ABS(numero - numero_db) * lat)) / SUM(1/ABS(numero - numero_db)) AS lat,
        SUM((1/ABS(numero - numero_db) * lon)) / SUM(1/ABS(numero - numero_db)) AS lon,
        '{match_type}' AS tipo_resultado,
         REGEXP_REPLACE(FIRST(endereco_encontrado), ', \\d+ -', CONCAT(', ', FIRST(numero), ' (aprox) -')) AS endereco_encontrado,
      {additional_cols}
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
