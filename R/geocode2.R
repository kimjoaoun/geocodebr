# @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#
# # open input data
# data_path <- system.file("extdata/sample_1.csv", package = "geocodebr")
# input_df <- read.csv(data_path)
#
# # df <- geocodebr::geocode(
# #   input_table = input_df,
# #   logradouro = "nm_logradouro",
# #   numero = "Numero",
# #   complemento = "Complemento",
# #   cep = "Cep",
# #   bairro = "Bairro",
# #   municipio = "nm_municipio",
# #   estado = "nm_uf"
# #   )
#
geocode2 <- function(addresses_table,
                    address_fields = setup_address_fields(),
                    n_cores = 1,
                    progress = TRUE,
                    cache = TRUE) {
  assert_address_fields(address_fields, addresses_table)
  checkmate::assert_data_frame(addresses_table)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)
  checkmate::assert_logical(progress, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)

  # standardizing the addresses table to increase the chances of finding a match
  # in the CNEFE data

  standard_locations <- enderecobr::padronizar_enderecos(
    addresses_table,
    campos_do_endereco = enderecobr::correspondencia_campos(
      logradouro = address_fields[["logradouro"]],
      numero = address_fields[["numero"]],
      complemento = address_fields[["complemento"]],
      cep = address_fields[["cep"]],
      bairro = address_fields[["bairro"]],
      municipio = address_fields[["municipio"]],
      estado = address_fields[["estado"]]
    ),
    formato_estados = "sigla"
  )

  # downloading cnefe. we only need to download the states present in the
  # addresses table, which may save us some time. we also subset cnefe to
  # include only the municipalities present in the input table, reducing the
  # search scope and consequently reducing processing time and memory usage

  present_states <- unique(standard_locations$estado_padr)

  cnefe_path <- download_cnefe(
    present_states,
    progress = progress,
    cache = cache
  )
  cnefe <- arrow::open_dataset(cnefe_path)

  # creating a temporary db and registering both the input table and the cnefe
  # data

  tmpdb <- tempfile(fileext = ".duckdb")
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = tmpdb)

  DBI::dbExecute(con, glue::glue("SET threads = {n_cores}"))

  duckdb::dbWriteTable(
    con,
    name = "standard_locations",
    value = standard_locations,
    temporary = TRUE
  )

  unique_muns <- unique(standard_locations$municipio_padr)
  muns_list <- paste(glue::glue("'{unique_muns}'"), collapse = ", ")

  duckdb::duckdb_register_arrow(con, "cnefe", cnefe)
  DBI::dbExecute(
    con,
    glue::glue(
      "CREATE OR REPLACE VIEW filtered_cnefe AS ",
      "SELECT * FROM cnefe WHERE municipio IN ({muns_list})"
    )
  )

  # to find the coordinates of the addresses, we merge the input table with the
  # cnefe data. the column names used in the input table are different than the
  # ones used in cnefe, so we create a helper object to "translate" the column
  # names between datasets

  equivalent_colnames <- tibble::tribble(
    ~standard_locations, ~cnefe,
    "logradouro_padr",   "logradouro_sem_numero",
    "numero_padr",       "numero",
    "complemento_padr",  "complemento",   # REMOVE, NOT ACTUALLY IN CNEFE
    "cep_padr",          "cep",
    "bairro_padr",       "localidade",
    "municipio_padr",    "municipio",
    "estado_padr",       "estado"         # INCLUDE, NOT IN CNEFE YET
  )

  lookup_vector <- equivalent_colnames$cnefe
  names(lookup_vector) <- equivalent_colnames$standard_locations

  # when merging the data, we have several different cases with different confidence
  # levels. from best to worst, they are:
  #
  # - case 01: match municipio, logradouro, numero, cep, localidade
  # - case 02: match municipio, logradouro, numero, cep
  # - case 03: match municipio, logradouro, numero, localidade
  # - case 04: match municipio, logradouro, cep, localidade
  # - case 05: match municipio, logradouro, numero
  # - case 06: match municipio, logradouro, cep
  # - case 07: match municipio, logradouro, localidade
  # - case 08: match municipio, logradouro
  # - case 09: match municipio, cep, localidade
  # - case 10: match municipio, cep
  # - case 11: match municipio, localidade
  # - case 12: match municipio

  DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN match_type VARCHAR DEFAULT NULL")
  DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN lon DOUBLE DEFAULT NULL")
  DBI::dbExecute(con, "ALTER TABLE standard_locations ADD COLUMN lat DOUBLE DEFAULT NULL")

  if (progress) {
    cli::cli_progress_bar(
      total = nrow(standard_locations),
      format = "Matched addresses: {formatC(cli::pb_current, big.mark = ',', format = 'd')}/{formatC(cli::pb_total, big.mark = ',', format = 'd')} {cli::pb_bar} {cli::pb_percent} - {cli::pb_status}",
      clear = FALSE
    )
    n_rows_affected <- 0
  }


  for (case in 1:12) {
    relevant_cols <- get_relevant_cols(case)
    formatted_case <- formatC(case, width = 2, flag = "0")

    if (progress) {
      cli::cli_progress_update(
        inc = n_rows_affected,
        status = glue::glue("Looking for case {formatted_case} matches"),
        force = TRUE
      )
    }

    if (all(relevant_cols %in% names(standard_locations))) {
      join_condition <- paste(
        glue::glue("standard_locations.{relevant_cols} = aggregated_cnefe.{lookup_vector[relevant_cols]}"),
        collapse = " AND "
      )

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

  if (progress) {
    cli::cli_progress_update(
      inc = n_rows_affected,
      status = "Done!",
      force = TRUE
    )
  }

  cols_to_keep <- names(standard_locations)
  cols_to_keep <- cols_to_keep[!grepl("_padr$", cols_to_keep)]
  cols_to_keep <- c(cols_to_keep, "match_type", "lon", "lat")
  cols_to_keep <- paste(cols_to_keep, collapse = ", ")

  output <- dplyr::tbl(
    con,
    dplyr::sql(glue::glue("SELECT {cols_to_keep} FROM standard_locations"))
  )
  output <- dplyr::collect(output)

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
      "complemento",
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

get_relevant_cols <- function(case) {
  relevant_cols <- if (case == 1) {
    c("municipio_padr", "logradouro_padr", "numero_padr", "cep_padr", "bairro_padr")
  } else if (case == 2) {
    c("municipio_padr", "logradouro_padr", "numero_padr", "cep_padr")
  } else if (case == 3) {
    c("municipio_padr", "logradouro_padr", "numero_padr", "bairro_padr")
  } else if (case == 4) {
    c("municipio_padr", "logradouro_padr", "cep_padr", "bairro_padr")
  } else if (case == 5) {
    c("municipio_padr", "logradouro_padr", "numero_padr")
  } else if (case == 6) {
    c("municipio_padr", "logradouro_padr", "cep_padr")
  } else if (case == 7) {
    c("municipio_padr", "logradouro_padr", "bairro_padr")
  } else if (case == 8) {
    c("municipio_padr", "logradouro_padr")
  } else if (case == 9) {
    c("municipio_padr", "cep_padr", "bairro_padr")
  } else if (case == 10) {
    c("municipio_padr", "cep_padr")
  } else if (case == 11) {
    c("municipio_padr", "bairro_padr")
  } else if (case == 12) {
    c("municipio_padr")
  }

  return(relevant_cols)
}
