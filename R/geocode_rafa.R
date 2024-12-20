#' Geocoding addresses based on CNEFE data
#'
#' @description
#' Takes a data frame containing addresses as an input and returns the spatial
#' coordinates found based on CNEFE data.
#'
#' @param input_table A data frame.
#' @param logradouro A string.
#' @param numero A string.
#' @param cep A string.
#' @param bairro A string.
#' @param municipio A string.
#' @param estado A string.
#' @param output_simple Logic. Defaults to `TRUE`
#' @template n_cores
#' @template progress
#' @template cache
#'
#' @return An arrow `Dataset` or a `"data.frame"` object.
#'
#' @family Microdata
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#'
#' # open input data
#' data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
#' input_df <- read.csv(data_path)
#'
#' df_geo <- geocodebr:::geocode_rafa(
#'    input_table = input_df,
#'    logradouro = "nm_logradouro",
#'    numero = "Numero",
#'    cep = "Cep",
#'    bairro = "Bairro",
#'    municipio = "nm_municipio",
#'    estado = "nm_uf"
#'    )
#'
geocode_rafa <- function(input_table,
                         logradouro = NULL,
                         numero = NULL,
                         cep = NULL,
                         bairro = NULL,
                         municipio = NULL,
                         estado = NULL,
                         progress = TRUE,
                         output_simple = TRUE,
                         n_cores = NULL,
                         cache = TRUE){

  # check input
  checkmate::assert_data_frame(input_table)
  checkmate::assert_logical(progress)
  checkmate::assert_logical(output_simple)
  checkmate::assert_number(n_cores, null.ok = TRUE)
  checkmate::assert_logical(cache)
  checkmate::assert_names(
    names(input_table),
    must.include = "id"
  )


  # normalize input data -------------------------------------------------------

  message("Standardizing input addresses")

  # correspondence of column names
  campos <- enderecobr::correspondencia_campos(
    logradouro = logradouro,
    numero = numero,
    cep = cep,
    bairro = bairro,
    municipio = municipio,
    estado = estado
  )

  # padroniza input do usuario
  input_padrao_raw <- enderecobr::padronizar_enderecos(
    enderecos = input_table,
    campos_do_endereco = campos,
    formato_estados = 'sigla'
  )

  # keep and rename colunms of input_padrao
  # keeping same column names used in our cnefe data set
  cols_padr <- grep("_padr", names(input_padrao_raw), value = TRUE)
  input_padrao <- input_padrao_raw[, .SD, .SDcols = c("id", cols_padr)]
  names(input_padrao) <- c("id", gsub("_padr", "", cols_padr))

  data.table::setnames(input_padrao, old = 'logradouro', new = 'logradouro_sem_numero')
  data.table::setnames(input_padrao, old = 'bairro', new = 'localidade')


  # create db connection -------------------------------------------------------
  con <- create_geocodebr_db(n_cores = n_cores)


  # add input and cnefe data sets to db --------------------------------------

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                       temporary = TRUE)

  input_states <- unique(input_padrao$estado)


  # download cnefe
  download_success <- download_cnefe(
    state = input_states,
    progress = progress,
    cache = cache
    )

  # check if download worked
  if (isFALSE(download_success)) { return(invisible(NULL)) }



  # Narrow search scope in cnefe to municipalities and zip codes present in input
  # input_ceps <- unique(input_padrao$cep)
  # input_ceps <- input_ceps[!is.na(input_ceps)]
  # if(is.null(input_ceps)){ input_ceps <- "*"}

  input_municipio <- unique(input_padrao$municipio)
  input_municipio <- input_municipio[!is.na(input_municipio)]
  if(is.null(input_municipio)){ input_municipio <- "*"}

  # Load CNEFE data and write to DuckDB
  filtered_cnefe <- arrow::open_dataset(get_cache_dir()) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()

  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)


  # START DETERMINISTIC MATCHING -----------------------------------------------

  message("Finding spatial coordinates")

  # - case 01: match municipio, logradouro, numero, cep, localidade
  # - case 02: match municipio, logradouro, numero, cep
  # - case 03: match municipio, logradouro, numero, localidade
  # - case 04: match municipio, logradouro, numero
  # - case 05: match municipio, logradouro, cep, localidade
  # - case 06: match municipio, logradouro, cep
  # - case 07: match municipio, logradouro, localidade
  # - case 08: match municipio, logradouro
  # - case 09: match municipio, cep, localidade
  # - case 10: match municipio, cep
  # - case 11: match municipio, localidade
  # - case 12: match municipio


  # key columns
  cols_01 <- c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "localidade")
  cols_02 <- c("estado", "municipio", "logradouro_sem_numero", "numero", "cep")
  cols_03 <- c("estado", "municipio", "logradouro_sem_numero", "numero", "localidade")
  cols_04 <- c("estado", "municipio", "logradouro_sem_numero", "numero")
  cols_05 <- c("estado", "municipio", "logradouro_sem_numero", "cep", "localidade")
  cols_06 <- c("estado", "municipio", "logradouro_sem_numero", "cep")
  cols_07 <- c("estado", "municipio", "logradouro_sem_numero", "localidade")
  cols_08 <- c("estado", "municipio", "logradouro_sem_numero")
  cols_09 <- c("estado", "municipio", "cep", "localidade")
  cols_10 <- c("estado", "municipio", "cep")
  cols_11 <- c("estado", "municipio", "localidade")
  cols_12 <- c("estado", "municipio")

  # start progress bar
  if (isTRUE(progress)) {
    total_n <- nrow(input_table)
    pb <- utils::txtProgressBar(min = 0, max = total_n, style = 3)

    ndone <- 0
    utils::setTxtProgressBar(pb, ndone)
    }



  ## CASE 1 --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, logradouro, numero, cep, bairro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe',
      output_tb = 'output_caso_01',
      key_cols <- cols_01,
      match_type = 1L
      )

    # UPDATE input_padrao_db: Remove observations found in previous step
    update_input_db(
      con,
      update_tb = 'input_padrao_db',
      reference_tb = 'output_caso_01'
      )

    # update progress bar
    if (isTRUE(progress)) {
      ndone <- temp_n
      utils::setTxtProgressBar(pb, ndone)
      }
  }
  # DBI::dbReadTable(con, 'output_caso_01')
  # DBI::dbRemoveTable(con, 'output_caso_01')


  ## CASE 2 --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, logradouro, numero, cep)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe',
      output_tb = 'output_caso_02',
      key_cols <- cols_02,
      match_type = 2L
    )

    # UPDATE input_padrao_db: Remove observations found in previous step
    update_input_db(
      con,
      update_tb = 'input_padrao_db',
      reference_tb = 'output_caso_02'
    )

    # update progress bar
    if (isTRUE(progress)) {
      ndone <- ndone + temp_n
      utils::setTxtProgressBar(pb, ndone)
    }
  }

  ## CASE 3 --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, logradouro, numero, bairro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe',
      output_tb = 'output_caso_03',
      key_cols <- cols_03,
      match_type = 3L
    )

    # UPDATE input_padrao_db: Remove observations found in previous step
    update_input_db(
      con,
      update_tb = 'input_padrao_db',
      reference_tb = 'output_caso_03'
    )

    # update progress bar
    if (isTRUE(progress)) {
      ndone <- ndone + temp_n
      utils::setTxtProgressBar(pb, ndone)
    }
  }

  ## CASE 4 --------------------------------------------------------------------


  # check if we have all required inputs
  inputs <- list(estado, municipio, logradouro, cep, bairro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
    con,
    x = 'input_padrao_db',
    y = 'filtered_cnefe',
    output_tb = 'output_caso_04',
    key_cols <- cols_04,
    match_type = 4L
  )

  # UPDATE input_padrao_db: Remove observations found in previous step
  update_input_db(
    con,
    update_tb = 'input_padrao_db',
    reference_tb = 'output_caso_04'
  )

  # update progress bar
  if (isTRUE(progress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
  }
  }


  ## CASE 5  --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, logradouro, numero)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){


    temp_n <- match_aggregated_cases(
    con,
    x = 'input_padrao_db',
    y = 'filtered_cnefe',
    output_tb = 'output_caso_05',
    key_cols <- cols_05,
    match_type = 5L
  )

  # UPDATE input_padrao_db: Remove observations found in previous step
  update_input_db(
    con,
    update_tb = 'input_padrao_db',
    reference_tb = 'output_caso_05'
  )

  # update progress bar
  if (isTRUE(progress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
  }
  }



  ## CASE 6  --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, logradouro, cep)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe',
      output_tb = 'output_caso_06',
      key_cols <- cols_06,
      match_type = 6L
    )

    # UPDATE input_padrao_db: Remove observations found in previous step
    update_input_db(
      con,
      update_tb = 'input_padrao_db',
      reference_tb = 'output_caso_06'
    )

    # update progress bar
    if (isTRUE(progress)) {
      ndone <- ndone + temp_n
      utils::setTxtProgressBar(pb, ndone)
    }
  }

  ## CASE 7 --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, logradouro, bairro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

      temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe',
      output_tb = 'output_caso_07',
      key_cols <- cols_07,
      match_type = 7L
    )

    # UPDATE input_padrao_db: Remove observations found in previous step
    update_input_db(
      con,
      update_tb = 'input_padrao_db',
      reference_tb = 'output_caso_07'
    )

    # update progress bar
    if (isTRUE(progress)) {
      ndone <- ndone + temp_n
      utils::setTxtProgressBar(pb, ndone)
    }
  }

  ## CASE 8 --------------------------------------------------------------------


  # check if we have all required inputs
  inputs <- list(estado, municipio, logradouro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe',
      output_tb = 'output_caso_08',
      key_cols <- cols_08,
      match_type = 8L
    )

    # UPDATE input_padrao_db: Remove observations found in previous step
    update_input_db(
      con,
      update_tb = 'input_padrao_db',
      reference_tb = 'output_caso_08'
    )

    # update progress bar
    if (isTRUE(progress)) {
      ndone <- ndone + temp_n
      utils::setTxtProgressBar(pb, ndone)
    }
  }

  ## CASE 9 --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, cep, bairro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe',
      output_tb = 'output_caso_09',
      key_cols <- cols_09,
      match_type = 9L
    )

    # UPDATE input_padrao_db: Remove observations found in previous step
    update_input_db(
      con,
      update_tb = 'input_padrao_db',
      reference_tb = 'output_caso_09'
    )

    # update progress bar
    if (isTRUE(progress)) {
      ndone <- ndone + temp_n
      utils::setTxtProgressBar(pb, ndone)
    }
  }

  ## CASE 10 --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, cep)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe',
      output_tb = 'output_caso_10',
      key_cols <- cols_10,
      match_type = 10L
    )

    # UPDATE input_padrao_db: Remove observations found in previous step
    update_input_db(
      con,
      update_tb = 'input_padrao_db',
      reference_tb = 'output_caso_10'
    )

    # update progress bar
    if (isTRUE(progress)) {
      ndone <- ndone + temp_n
      utils::setTxtProgressBar(pb, ndone)
    }
  }

  ## CASE 11 --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, bairro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe',
      output_tb = 'output_caso_11',
      key_cols <- cols_11,
      match_type = 11L
    )

    # UPDATE input_padrao_db: Remove observations found in previous step
    update_input_db(
      con,
      update_tb = 'input_padrao_db',
      reference_tb = 'output_caso_11'
    )

    # update progress bar
    if (isTRUE(progress)) {
      ndone <- ndone + temp_n
      utils::setTxtProgressBar(pb, ndone)
    }
  }

  ## CASE 12 --------------------------------------------------------------------

    # check if we have all required inputs
    inputs <- list(estado, municipio)
    all_required_inputs <- !any(sapply(inputs, is.null))

    if(all_required_inputs){

      temp_n <- match_aggregated_cases(
        con,
        x = 'input_padrao_db',
        y = 'filtered_cnefe',
        output_tb = 'output_caso_12',
        key_cols <- cols_12,
        match_type = 12L
      )

      # UPDATE input_padrao_db: Remove observations found in previous step
      update_input_db(
        con,
        update_tb = 'input_padrao_db',
        reference_tb = 'output_caso_12'
      )

      # update progress bar
      if (isTRUE(progress)) {
        ndone <- ndone + temp_n
        utils::setTxtProgressBar(pb, ndone)
        base::close(pb)
      }
    }


    ## CASE 999 --------------------------------------------------------------------
  # TO DO
  # WHAT SHOULD BE DONE FOR CASES NOT FOUND ?
  # AND THEIR EFFECT ON THE PROGRESS BAR


  # DBI::dbReadTable(con, 'input_padrao_db')
  # DBI::dbReadTable(con, 'output_caso_01')

  # DBI::dbRemoveTable(con, 'output_caso_01')





  # prepare output -----------------------------------------------

  # THIS NEEDS TO BE IMPROVED / optimized
  # THIS NEEDS TO BE IMPROVED / optimized

  # list all table outputs
  all_output_tbs <- c(
    ifelse( DBI::dbExistsTable(con, 'output_caso_01'), 'output_caso_01', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_02'), 'output_caso_02', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_03'), 'output_caso_03', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_04'), 'output_caso_04', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_05'), 'output_caso_05', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_06'), 'output_caso_06', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_07'), 'output_caso_07', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_08'), 'output_caso_08', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_09'), 'output_caso_09', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_10'), 'output_caso_10', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_11'), 'output_caso_11', 'empty'),
    ifelse( DBI::dbExistsTable(con, 'output_caso_12'), 'output_caso_12', 'empty')
  )
  all_output_tbs <- all_output_tbs[!grepl('empty', all_output_tbs)]

  # save output to db
  output_query <- paste("CREATE TEMPORARY TABLE output_db AS",
                        paste0("SELECT ", paste0('*', " FROM ", all_output_tbs),
                               collapse = " UNION ALL "))

  DBI::dbExecute(con, output_query)

  # output with only id and geocode columns
  if (isTRUE(output_simple)) {

    output_deterministic <- DBI::dbReadTable(con, 'output_db')
  }

  # output with all original columns
  if (isFALSE(output_simple)){

    duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                        temporary = TRUE, overwrite=TRUE)

    x_columns <- names(input_padrao)

    output_deterministic <- merge_results(con,
                    x='input_padrao_db',
                    y='output_db',
                    key_column='id',
                    select_columns = x_columns)

  }

  # Disconnect from DuckDB when done
  duckdb::duckdb_unregister_arrow(con, 'cnefe')
  duckdb::dbDisconnect(con, shutdown=TRUE)
  # gc()

  # Return the result
  return(output_deterministic)

  #   # NEXT STEPS
  #   - optimize disk and parallel operations in duckdb
  #   - cases that matche first 7 digits of CEP
  #   - exceptional cases (no info on municipio input)
  #   - join probabilistico
  #   - interpolar numeros na mesma rua

}

