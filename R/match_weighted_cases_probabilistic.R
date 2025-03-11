match_weighted_cases_probabilistic <- function( # nocov start
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

  # Load cnefe CNEFE data without numbers to speed up probabilistic match
  filtered_cnefe_logradouros <- paste0(
    geocodebr::listar_pasta_cache(),
    "/municipio_logradouro_cep_localidade.parquet"
    ) |>
    arrow_open_dataset() |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)
  duckdb::duckdb_register_arrow(con, "filtered_cnefe_logradouros", filtered_cnefe_logradouros)

  # cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # remove numero and logradouro from key cols to allow for the matching
  key_cols <- key_cols[key_cols != 'numero']
  key_cols <- key_cols[key_cols != 'logradouro']

  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  join_condition_lookup <- paste(
    glue::glue("temp_unique_logradouros.{key_cols} = {x}.{key_cols}"),
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
  min_cutoff <- get_prob_match_cutoff(match_type)


  # 1st step: create small table with unique logradouros -----------------------

  query_unique_logradouros <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_unique_logradouros AS
      SELECT DISTINCT {paste(c(key_cols, 'logradouro'), collapse = ', ')}
      FROM filtered_cnefe_logradouros
    ORDER BY {paste(c(key_cols, 'logradouro'), collapse = ', ')};"
    )

  DBI::dbExecute(con, query_unique_logradouros)
  # a <- DBI::dbReadTable(con, 'temp_unique_logradouros')




  # 2nd step: update input_padrao_db with the most probable logradouro ---------

  query_lookup <- glue::glue(
    "WITH ranked_data AS (
    SELECT
      {x}.tempidgeocodebr,
      {x}.logradouro AS logradouro,
      temp_unique_logradouros.logradouro AS logradouro_cnefe,
      jaro_similarity({x}.logradouro, temp_unique_logradouros.logradouro) AS similarity,
      RANK() OVER (
        PARTITION BY {x}.tempidgeocodebr
        ORDER BY jaro_similarity({x}.logradouro, temp_unique_logradouros.logradouro) DESC
      ) AS rank
    FROM {x}
    JOIN temp_unique_logradouros
      ON {join_condition_lookup}
    WHERE {cols_not_null}
  )

  UPDATE {x}
    SET temp_lograd_determ = ranked_data.logradouro_cnefe,
        similaridade_logradouro = similarity
    FROM ranked_data
  WHERE {x}.tempidgeocodebr = ranked_data.tempidgeocodebr
    AND similarity > {min_cutoff}
    AND rank = 1;"
  )

  DBI::dbExecute(con, query_lookup)
  # c <- DBI::dbReadTable(con, 'input_padrao_db')



  # 3rd step: match deterministico --------------------------------------------------------

  # update join condition to use probable logradouro
  join_condition_match <- paste(
    join_condition,
    glue::glue("AND {x}.temp_lograd_determ = {y}.logradouro")
    )

  # cols that cannot be null
  cols_not_null_match <- paste(
    cols_not_null,
    glue::glue("AND {x}.temp_lograd_determ IS NOT NULL")
  )

  # match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS
    SELECT {x}.tempidgeocodebr, {x}.numero, {y}.numero AS numero_cnefe,
    {y}.lat, {y}.lon,
    REGEXP_REPLACE( {y}.endereco_completo, ', \\d+ -', CONCAT(', ', {x}.numero, ' (aprox) -')) AS endereco_encontrado,
    {x}.similaridade_logradouro,
    {y}.n_casos AS contagem_cnefe {additional_cols}
    FROM {x}
    LEFT JOIN {y}
    ON {join_condition_match}
    WHERE {cols_not_null_match} AND {x}.numero IS NOT NULL;"
  )

  DBI::dbExecute(con, query_match)
  # c <- DBI::dbReadTable(con, 'temp_db')



  # 4th step: aggregate --------------------------------------------------------

  # summarize query
  query_aggregate <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado, endereco_encontrado, contagem_cnefe)
      SELECT tempidgeocodebr,
      SUM((1/ABS(numero - numero_cnefe) * lat)) / SUM(1/ABS(numero - numero_cnefe)) AS lat,
      SUM((1/ABS(numero - numero_cnefe) * lon)) / SUM(1/ABS(numero - numero_cnefe)) AS lon,
      '{match_type}' AS tipo_resultado,
      FIRST(endereco_encontrado) AS endereco_encontrado,
      FIRST(contagem_cnefe) AS contagem_cnefe
      FROM temp_db
      GROUP BY tempidgeocodebr, endereco_encontrado;"
  )



  if (isTRUE(resultado_completo)) {

    additional_cols <- paste0(
      glue::glue("FIRST({key_cols}_encontrado)"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols)

    query_aggregate <- glue::glue(
    #  "CREATE OR REPLACE TEMPORARY TABLE aaa AS
     "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado,
                            endereco_encontrado, similaridade_logradouro,
                            contagem_cnefe {colunas_encontradas})
       SELECT tempidgeocodebr,
        SUM((1/ABS(numero - numero_cnefe) * lat)) / SUM(1/ABS(numero - numero_cnefe)) AS lat,
        SUM((1/ABS(numero - numero_cnefe) * lon)) / SUM(1/ABS(numero - numero_cnefe)) AS lon,
        '{match_type}' AS tipo_resultado,
        FIRST(endereco_encontrado) AS endereco_encontrado,
        FIRST(similaridade_logradouro),
        FIRST(contagem_cnefe) AS contagem_cnefe
        {additional_cols}
      FROM temp_db
      GROUP BY tempidgeocodebr, endereco_encontrado;"
    )
  }

  DBI::dbExecute(con, query_aggregate)
  # d <- DBI::dbReadTable(con, 'output_db')
  # d <- DBI::dbReadTable(cn, 'aaa')

  # remove arrow tables from db
  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")
  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe_logradouros")

  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end
