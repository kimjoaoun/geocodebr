#' Geocoding addresses based on CNEFE data
#'
#' @description
#' Takes a data frame containing addresses as an input and returns the spatial
#' coordinates found based on CNEFE data.
#'
#' @param addresses_table A data frame. The addresses to be geocoded. Each
#'   column must represent an address field.
#' @param address_fields A character vector. The correspondence between each
#'   address field and the name of the column that describes it in
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
#' @return An arrow `Dataset` or a `"data.frame"` object.
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
#'#df <- geocodebr:::geocode_rafa(
#'#  addresses_table = input_df,
#'#  address_fields = fields,
#'#  progress = FALSE
#'#  )
#'#
#'#head(df)
#'
geocode_rafa <- function(addresses_table,
                              address_fields = setup_address_fields(),
                              n_cores = 1,
                              progress = TRUE,
                              cache = TRUE){

  # check input
  assert_address_fields(address_fields, addresses_table)
  checkmate::assert_data_frame(addresses_table)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)
  checkmate::assert_logical(progress, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)


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
    formato_estados = "sigla"
  )

  # keep and rename colunms of input_padrao to use the
  # same column names used in cnefe data set
  data.table::setDT(input_padrao)
  cols_padr <- grep("_padr", names(input_padrao), value = TRUE)
  input_padrao <- input_padrao[, .SD, .SDcols = c(cols_padr)]
  names(input_padrao) <- c(gsub("_padr", "", cols_padr))

  data.table::setnames(
    x = input_padrao,
    old = c('logradouro', 'bairro'),
    new = c('logradouro_sem_numero', 'localidade'))

  # temp id
  input_padrao[, tempidgeocodebr := 1:nrow(input_padrao) ]

  ### temporary
  input_padrao[, numero := as.numeric(numero)]


  # downloading cnefe. we only need to download the states present in the
  # addresses table, which may save us some time.
  input_states <- unique(input_padrao$estado)
  cnefe_dir <- download_cnefe(
    progress = progress,
    cache = cache
  )


  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                       overwrite = TRUE, temporary = TRUE)


  # register cnefe data to db, but only include states and municipalities
  # present in the input table, reducing the search scope and consequently
  # reducing processing time and memory usage

  input_municipio <- unique(input_padrao$municipio)
  input_municipio <- input_municipio[!is.na(input_municipio)]
  if(is.null(input_municipio)){ input_municipio <- "*"}

  # Load CNEFE data and write to DuckDB
  filtered_cnefe <- arrow::open_dataset(get_cache_dir()) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::mutate(numero = dplyr::if_else(numero=='S/N', NA, numero)) |>
    dplyr::mutate(numero = as.numeric(numero)) |>
    dplyr::compute()

  # 6666
  # this is necessary to use match with weighted cases
   duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)
  # #filtered_cnefe <- dplyr::collect(filtered_cnefe)
  # #duckdb::dbWriteTable(con, "filtered_cnefe", filtered_cnefe,
  # #                     temporary = TRUE, overwrite = TRUE)
  #
  #
  #  DBI::dbExecute(
  #    con,
  #    glue::glue("UPDATE filtered_cnefe SET numero = TRY_CAST(numero AS INTEGER);
  #            ALTER TABLE filtered_cnefe ALTER COLUMN numero TYPE INTEGER USING TRY_CAST(numero AS INTEGER);")
  #  )


  # START DETERMINISTIC MATCHING -----------------------------------------------

  # when merging the data, we have several different cases with different confidence
  # levels. from best to worst, they are:

  # - case 01: match municipio, logradouro, numero, cep, localidade
  # - case 02: match municipio, logradouro, numero, cep
  # - case 03: match municipio, logradouro, numero, localidade
  # - case 04: match municipio, logradouro, numero
  # - case 44: match municipio, logradouro, numero (interpolado)
  # - case 05: match municipio, logradouro, cep, localidade
  # - case 06: match municipio, logradouro, cep
  # - case 07: match municipio, logradouro, localidade
  # - case 08: match municipio, logradouro
  # - case 09: match municipio, cep, localidade
  # - case 10: match municipio, cep
  # - case 11: match municipio, localidade
  # - case 12: match municipio

  if (progress) {
    prog <- create_progress_bar(input_padrao)
    n_rows_affected <- 0

    message_looking_for_matches()
  }


  for (case in c(1:4, 44, 5:12)) {

    relevant_cols <- get_relevant_cols_rafa(case)
    formatted_case <- formatC(case, width = 2, flag = "0")

    if (progress) update_progress_bar(n_rows_affected, formatted_case)


    if (all(relevant_cols %in% names(input_padrao))) {

      # select match function
      match_fun <- ifelse(case != 44, match_cases, match_weighted_cases)

      n_rows_affected <- match_fun(
        con,
        x = 'input_padrao_db',
        y = 'filtered_cnefe',
        output_tb = paste0('output_caso_', formatted_case),
        key_cols = relevant_cols,
        match_type = case
      )
    }

  }

  if (progress) finish_progress_bar(n_rows_affected)



  # prepare output -----------------------------------------------
  # THIS could BE IMPROVED / optimized

  # list all table outputs
  all_possible_tables <- glue::glue("output_caso_{formatC(c(1:12,44), width = 2, flag = '0')}")

  # check which tables have been created
  output_tables <- lapply(
    X= all_possible_tables,
    FUN = function(i){ ifelse( DBI::dbExistsTable(con, i), i, 'empty') }) |>
    unlist()

  all_output_tbs <- output_tables[!grepl('empty', output_tables)]

  # save output to db
  output_query <- paste("CREATE TEMPORARY TABLE output_db AS",
                        paste0("SELECT ", paste0('*', " FROM ", all_output_tbs),
                               collapse = " UNION ALL ")
                        )

  DBI::dbExecute(con, output_query)

  # add precision column
  DBI::dbExecute(
    con,
    glue::glue("ALTER TABLE output_db ADD COLUMN precision TEXT;")
  )

  # output with all original columns
  duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                       temporary = TRUE, overwrite=TRUE)

  x_columns <- names(input_padrao)

  output_deterministic <- merge_results(
    con,
    x='input_padrao_db',
    y='output_db',
    key_column='tempidgeocodebr',
    select_columns = x_columns
  )

  # Disconnect from DuckDB when done
  duckdb::dbDisconnect(con, shutdown=TRUE)

  # Return the result
  return(output_deterministic)
}
