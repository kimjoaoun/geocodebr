match_cases_probabilistic <- function(
    con = con,
    x = 'input_padrao_db',
    y = 'filtered_cnefe',
    output_tb = "output_db",
    key_cols = key_cols,
    match_type = match_type,
    resultado_completo){

  # get corresponding parquet table
  table_name <- get_reference_table(key_cols, match_type)

  # build path to local file
  path_to_parquet <- paste0(listar_pasta_cache(), "/", table_name, ".parquet")

  # determine geographical scope of the search
  input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado
  input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio

  # Load CNEFE data and write to DuckDB
  # filter cnefe to include only states and municipalities
  # present in the input table, reducing the search scope
  filtered_cnefe <- arrow_open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()


  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)

  # cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # remove logradouro
  key_cols <- key_cols[key_cols != 'logradouro']


  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )


  # whether to keep all columns in the result
  colunas_encontradas <- ""
  additional_cols <- ""

  if (isTRUE(resultado_completo)) {

    colunas_encontradas <- paste0(
      glue::glue("{key_cols}_encontrado"),
      collapse = ', ')

    colunas_encontradas <- gsub('localidade_encontrado', 'localidade_encontrada', colunas_encontradas)
    colunas_encontradas <- paste0(", ", colunas_encontradas)

    additional_cols <- paste0(
      glue::glue("filtered_cnefe.{key_cols} AS {key_cols}_encontrado"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols)
  }

  # min cutoff for string match
  min_cutoff <- ifelse(match_type %in% c('pn01', 'pi01', 'pr01'), .85, .9)

  # 1st step: match  --------------------------------------------------------

  # match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS
     WITH ranked_data AS (
      SELECT
      {x}.tempidgeocodebr, {y}.lat, {y}.lon,
      {x}.logradouro AS logradouro,
      {y}.logradouro AS logradouro_cnefe,
      {y}.endereco_completo AS endereco_encontrado,
      {y}.n_casos AS contagem_cnefe,
      jaro_similarity({x}.logradouro, {y}.logradouro) AS similarity,
      RANK() OVER (PARTITION BY {x}.tempidgeocodebr, endereco_encontrado ORDER BY similarity DESC) AS rank
      {additional_cols}
    FROM {x}
    LEFT JOIN {y}
      ON {join_condition}
      WHERE {cols_not_null} AND {y}.lon IS NOT NULL
      )
    SELECT *
      FROM ranked_data
      WHERE similarity > {min_cutoff}
      AND rank = 1;"
  )

  DBI::dbExecute(con, query_match)
  # a <- DBI::dbReadTable(con, 'temp_db')



  # 2nd step: update output table --------------------------------------------------------

  if (isTRUE(resultado_completo)) {

    colunas_encontradas <- paste0(colunas_encontradas, ", similaridade_logradouro")


    additional_cols <- paste0(
      glue::glue("temp_db.{key_cols}_encontrado"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols, ", temp_db.similarity AS similaridade_logradouro")
  }

  query_update_db <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado, endereco_encontrado, contagem_cnefe {colunas_encontradas})
      SELECT temp_db.tempidgeocodebr,
             temp_db.lat,
             temp_db.lon,
             '{match_type}' AS tipo_resultado,
             temp_db.endereco_encontrado,
             temp_db.contagem_cnefe {additional_cols}
      FROM temp_db
      WHERE temp_db.lon IS NOT NULL;"
  )

  DBI::dbExecute(con, query_update_db)


  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")

  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
}
