# prolema atual é que a pacote recalcula distancias de string TODA VEZ
# seria melhor criar uma tabela de distancia e ir populando

# 1st step: create small table with unique logradouros
# 2nd step: update input_padrao_db with the most probable logradouro
# 3rd step: deterministic match to update output

match_cases_probabilistic <- function(
    con = con,
    x = 'input_padrao_db',
    y = 'filtered_cnefe',
    output_tb = "output_db",
    key_cols = key_cols,
    match_type = match_type,
    resultado_completo){ # nocov start

  # get corresponding parquet table and key columns
  table_name <- get_reference_table(match_type)
  key_cols <- get_key_cols(match_type)

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



  # 1st step: create small table with unique logradouros -----------------------

  # check if view 'unique_logradouros_cep_localidade' exists
  temp_check <- duckdb::duckdb_list_arrow(conn = con)
  temp_check <- 'unique_logradouros_cep_localidade' %in% temp_check

  if (match_type == 'pn01' | isFALSE(temp_check)) {

    unique_logradouros_cep_localidade <- filtered_cnefe |>
      dplyr::select(dplyr::all_of(c("estado", "municipio", "logradouro", "cep", "localidade"))) |>
      dplyr::distinct() |>
      dplyr::compute()

    # register to db
    duckdb::duckdb_register_arrow(con, "unique_logradouros", unique_logradouros_cep_localidade)
    duckdb::duckdb_register_arrow(con, "unique_logradouros_cep_localidade", unique_logradouros_cep_localidade)
    # a <- DBI::dbReadTable(con, 'unique_logradouros')

  } else {

    # 666 esse passo poderia tmb filtar estados e municipios presentes
    unique_cols <- key_cols[!key_cols %in% "numero"]

    query_unique_logradouros <- glue::glue(
          "CREATE OR REPLACE VIEW unique_logradouros AS
            SELECT DISTINCT {paste(unique_cols, collapse = ', ')}
            FROM unique_logradouros_cep_localidade;"
          )

    DBI::dbSendQueryArrow(con, query_unique_logradouros)
  }


  # 2nd step: update input_padrao_db with the most probable logradouro ---------

  # cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )

  # remove numero and logradouro from key cols to allow for the matching
  key_cols_string_dist <- key_cols[!key_cols %in%  c("numero", "logradouro")]

  join_condition_lookup <- paste(
    glue::glue("unique_logradouros.{key_cols_string_dist} = {x}.{key_cols_string_dist}"),
    collapse = ' AND '
  )

  # min cutoff for string match
  min_cutoff <- get_prob_match_cutoff(match_type)

# filtro determi no cep e localidade

  # query
  query_lookup <- glue::glue(
    "WITH ranked_data AS (
    SELECT
      {x}.tempidgeocodebr,
      {x}.logradouro AS logradouro,
      unique_logradouros.logradouro AS logradouro_cnefe,
      CAST(jaro_similarity({x}.logradouro, unique_logradouros.logradouro) AS NUMERIC(5,3)) AS similarity,
      RANK() OVER (PARTITION BY {x}.tempidgeocodebr ORDER BY similarity DESC) AS rank
    FROM {x}
    JOIN unique_logradouros
      ON {join_condition_lookup}
    WHERE {cols_not_null} AND {x}.similaridade_logradouro IS NULL AND similarity > {min_cutoff}
  )

  UPDATE {x}
    SET temp_lograd_determ = ranked_data.logradouro_cnefe,
        similaridade_logradouro = similarity
    FROM ranked_data
  WHERE {x}.tempidgeocodebr = ranked_data.tempidgeocodebr
    AND similarity > {min_cutoff}
    AND rank = 1;"
  )

  DBI::dbSendQueryArrow(con, query_lookup)
  # DBI::dbExecute(con, query_lookup)
  # b <- DBI::dbReadTable(con, 'input_padrao_db')




  # 3rd step: update output table com match deterministico --------------------------------------------------------

  key_cols <- get_key_cols(match_type)

  # update join condition to use probable logradouro and deterministic number
  join_condition_match <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  join_condition_match <- gsub('input_padrao_db.logradouro', 'input_padrao_db.temp_lograd_determ', join_condition_match)

  # update cols that cannot be null
  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
    )

  # cols that cannot be null
  cols_not_null <- gsub('.logradouro', '.temp_lograd_determ', cols_not_null)

  # whether to keep all columns in the result
  colunas_encontradas <- ""
  additional_cols <- ""


  if (isTRUE(resultado_completo)) {

    colunas_encontradas <- paste0(
      glue::glue("{key_cols}_encontrado"),
      collapse = ', ')

    colunas_encontradas <- gsub('localidade_encontrado', 'localidade_encontrada', colunas_encontradas)
    colunas_encontradas <- paste0(", ", colunas_encontradas)
    colunas_encontradas <- paste0(colunas_encontradas, ", similaridade_logradouro")

    additional_cols <- paste0(
      glue::glue("filtered_cnefe.{key_cols} AS {key_cols}_encontrado"),
      collapse = ', ')

    additional_cols <- gsub('localidade_encontrado', 'localidade_encontrada', additional_cols)
    additional_cols <- paste0(", ", additional_cols, ", input_padrao_db.similaridade_logradouro AS similaridade_logradouro")
    }


  # summarize query
  query_update_db <- glue::glue(
    "INSERT INTO output_db (tempidgeocodebr, lat, lon, tipo_resultado, endereco_encontrado, contagem_cnefe {colunas_encontradas})
      SELECT {x}.tempidgeocodebr, filtered_cnefe.lat, filtered_cnefe.lon,
             '{match_type}' AS tipo_resultado, filtered_cnefe.endereco_completo AS endereco_encontrado,
             filtered_cnefe.n_casos AS contagem_cnefe {additional_cols}
      FROM {x}
      LEFT JOIN filtered_cnefe
      ON {join_condition_match}
      WHERE {cols_not_null} AND filtered_cnefe.lon IS NOT NULL;"
  )



  DBI::dbSendQueryArrow(con, query_update_db)
  # DBI::dbExecute(con, query_update_db)
  # c <- DBI::dbReadTable(con, 'output_db')


  # remove arrow tables from db
  duckdb::duckdb_unregister_arrow(con, "unique_logradouros")
  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")



  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end





# 666666666666666666666666666-------------------------------------------------------------------------------

match_cases_probabilistic_old <- function(
    con = con,
    x = 'input_padrao_db',
    y = 'filtered_cnefe',
    output_tb = "output_db",
    key_cols = key_cols,
    match_type = match_type,
    resultado_completo){ # nocov start

  # get corresponding parquet table
  table_name <- get_reference_table(match_type)
  key_cols <- get_key_cols(match_type)

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
  min_cutoff <- get_prob_match_cutoff(match_type)



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
  # b <- DBI::dbReadTable(con, 'output_db')


  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")

  # UPDATE input_padrao_db: Remove observations found in previous step
  temp_n <- update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
} # nocov end


#### compare code versions -------

# library(dplyr)
# dfgeo <- readRDS("dfgeo2.rds")
#
#
# match_type <- "pn03"
# idsss <- filter(dfgeo, tipo_resultado %in% match_type)$id
#
# input_padrao_temp <- filter(input_padrao, tempidgeocodebr %in% idsss)
#
# # Convert input data frame to DuckDB table
# duckdb::dbWriteTable(con, "input_padrao_db", input_padrao_temp,
#                      overwrite = TRUE, temporary = TRUE)
#
#
# key_cols <- get_key_cols(match_type)
#
# bench::mark(
#   match_cases_probabilistic(  # match_cases_probabilistic_old
#     con = con,
#     x = 'input_padrao_db',
#     y = 'filtered_cnefe',
#     output_tb = "output_db",
#     key_cols = key_cols,
#     match_type = match_type,
#     resultado_completo = TRUE)
# )
#
# c <- DBI::dbReadTable(con, 'output_db')
#
# # expression     min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory
# # current1     60.9ms 66.9ms      14.1     218KB     2.35     6     1      426ms <dbl>  <Rprofmem>
# # old1           56ms 64.4ms      15.2     225KB     2.18     7     1      460ms <dbl>  <Rprofmem>
