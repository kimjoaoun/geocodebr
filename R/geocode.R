#' Geocoding addresses based on CNEFE data
#'
#' @description
#' Takes a data frame containing addresses as an input and returns the spatial
#' coordinates found based on CNEFE data.
#'
#' @param input_table A data frame.
#' @param logradouro A string.
#' @param numero A string.
#' @param complemento A string.
#' @param cep A string.
#' @param bairro A string.
#' @param municipio A string.
#' @param estado A string.
#' @param output_simple Logic. Defaults to `TRUE`
#' @template ncores
#' @template progress
#' @template cache
#'
#' @return An arrow `Dataset` or a `"data.frame"` object.
#' @export
#' @family Microdata
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#'
#' # open input data
#' data_path <- system.file("extdata/sample_1.csv", package = "geocodebr")
#' input_df <- read.csv(data_path)
#'
#' # df <- geocodebr::geocode(
#' #   input_table = input_df,
#' #   logradouro = "nm_logradouro",
#' #   numero = "Numero",
#' #   complemento = "Complemento",
#' #   cep = "Cep",
#' #   bairro = "Bairro",
#' #   municipio = "nm_municipio",
#' #   estado = "nm_uf"
#' #   )
#'
geocode <- function(input_table,
                    logradouro = NULL,
                    numero = NULL,
                    complemento = NULL,
                    cep = NULL,
                    bairro = NULL,
                    municipio = NULL,
                    estado = NULL,
                    progress = TRUE,
                    output_simple = TRUE,
                    ncores = NULL,
                    cache = TRUE){

  # check input
  checkmate::assert_data_frame(input_table)
  checkmate::assert_logical(progress)
  checkmate::assert_logical(output_simple)
  checkmate::assert_number(ncores, null.ok = TRUE)
  checkmate::assert_logical(cache)
  checkmate::assert_names(
    names(input_table),
    must.include = "ID",
    .var.name = "input_table"
  )


  # normalize input data -------------------------------------------------------

  # correspondence of column names
  campos <- enderecobr::correspondencia_campos(
    logradouro = logradouro,
    numero = numero,
    complemento = complemento,
    cep = cep,
    bairro = bairro,
    municipio = municipio,
    estado = estado
  )

  # padroniza input do usuario
  input_padrao_raw <- enderecobr::padronizar_enderecos(
    enderecos = input_table,
    campos_do_endereco = campos
  )

  # keep and rename colunms of input_padrao
  # keeping same column names used in our cnefe data set
  cols_padr <- grep("_padr", names(input_padrao_raw), value = TRUE)
  input_padrao <- input_padrao_raw[, .SD, .SDcols = c("ID", cols_padr)]
  names(input_padrao) <- c("ID", gsub("_padr", "", cols_padr))

  data.table::setnames(input_padrao, old = 'logradouro', new = 'logradouro_sem_numero')
  data.table::setnames(input_padrao, old = 'bairro', new = 'localidade')


  # create db connection -------------------------------------------------------
  con <- create_geocodebr_db(ncores = ncores)



  # add input and cnefe data sets to db --------------------------------------

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                       temporary = TRUE, overwrite=TRUE)

  # add abbrev state
  add_abbrev_state_col(con, update_tb = "input_padrao_db")

  # determine states present in the input
  query <- "SELECT DISTINCT estado FROM input_padrao_db"
  input_states <- DBI::dbGetQuery(con, query)[[1]]

  # download cnefe
  download_success <- download_cnefe(
    state = input_states,
    progress = progress,
    cache = cache
    )

  # check if download worked
  if (isFALSE(download_success)) { return(invisible(NULL)) }

  # Load CNEFE data and write to DuckDB
  cnefe <- arrow_open_dataset(geocodebr_env$cache_dir)
  duckdb::duckdb_register_arrow(con, "cnefe", cnefe)

  # # more than 2x SLOWER
  # dir <- fs::path(geocodebr_env$cache_dir, "/**/*.parquet")
  # DBI::dbExecute(con,
  #           sprintf("CREATE TEMPORARY TABLE cnefe AS SELECT * FROM read_parquet('%s')",
  #                   dir))

  ##  DBI::dbRemoveTable(con, 'cnefe')


  # Narrow search scope in cnefe to municipalities and zip codes present in input
  input_ceps <- unique(input_padrao$cep)
  input_ceps <- input_ceps[!is.na(input_ceps)]
  if(is.null(input_ceps)){ input_ceps <- "*"}

  input_municipio <- unique(input_padrao$municipio)
  input_municipio <- input_municipio[!is.na(input_municipio)]
  if(is.null(input_municipio)){ input_municipio <- "*"}

  query_filter_cnefe_municipios <- sprintf("
  CREATE TEMPORARY TABLE filtered_cnefe_cep AS
  SELECT * FROM cnefe
  WHERE cep IN ('%s') OR municipio IN ('%s')",
                                   paste(input_ceps, collapse = "', '"),
                                   paste(input_municipio, collapse = "', '")
  )

  DBI::dbExecute(con, query_filter_cnefe_municipios)
  # duckdb::dbSendQuery(con, query_ceps_municipios)


  # START DETERMINISTIC MATCHING -----------------------------------------------

  # - case 01: match municipio, logradouro, numero, cep, localidade
  # - case 02: match municipio, logradouro, numero, cep
  # - case 03: match municipio, logradouro, cep, localidade
  # - case 04: match municipio, logradouro, numero
  # - case 05: match municipio, logradouro, cep
  # - case 06: match municipio, logradouro, localidade
  # - case 07: match municipio, logradouro
  # - case 08: match municipio, cep, localidade
  # - case 09: match municipio, cep
  # - case 10: match municipio, localidade
  # - case 11: match municipio


  # key columns
  cols_01 <- c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "localidade")
  cols_02 <- c("estado", "municipio", "logradouro_sem_numero", "numero", "cep")
  cols_03 <- c("estado", "municipio", "logradouro_sem_numero", "cep", "localidade")
  cols_04 <- c("estado", "municipio", "logradouro_sem_numero", "numero")
  cols_05 <- c("estado", "municipio", "logradouro_sem_numero", "cep")
  cols_06 <- c("estado", "municipio", "logradouro_sem_numero", "localidade")
  cols_07 <- c("estado", "municipio", "logradouro_sem_numero")
  cols_08 <- c("estado", "municipio", "cep", "localidade")
  cols_09 <- c("estado", "municipio", "cep")
  cols_10 <- c("estado", "municipio", "localidade")
  cols_11 <- c("estado", "municipio")

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
      y = 'filtered_cnefe_cep',
      output_tb = 'output_caso_01',
      key_cols <- cols_01,
      precision = 1L
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
      y = 'filtered_cnefe_cep',
      output_tb = 'output_caso_02',
      key_cols <- cols_02,
      precision = 2L
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
  inputs <- list(estado, municipio, logradouro, cep, bairro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
    con,
    x = 'input_padrao_db',
    y = 'filtered_cnefe_cep',
    output_tb = 'output_caso_03',
    key_cols <- cols_03,
    precision = 3L
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


  ## CASE 4  --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, logradouro, numero)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){


    temp_n <- match_aggregated_cases(
    con,
    x = 'input_padrao_db',
    y = 'filtered_cnefe_cep',
    output_tb = 'output_caso_04',
    key_cols <- cols_04,
    precision = 4L
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
  inputs <- list(estado, municipio, logradouro, cep)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe_cep',
      output_tb = 'output_caso_05',
      key_cols <- cols_05,
      precision = 5L
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

  ## CASE 6 --------------------------------------------------------------------

  # check if we have all required inputs
  inputs <- list(estado, municipio, logradouro, bairro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

      temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe_cep',
      output_tb = 'output_caso_06',
      key_cols <- cols_06,
      precision = 6L
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
  inputs <- list(estado, municipio, logradouro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe_cep',
      output_tb = 'output_caso_07',
      key_cols <- cols_07,
      precision = 7L
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
  inputs <- list(estado, municipio, cep, bairro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe_cep',
      output_tb = 'output_caso_08',
      key_cols <- cols_08,
      precision = 8L
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
  inputs <- list(estado, municipio, cep)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

    temp_n <- match_aggregated_cases(
      con,
      x = 'input_padrao_db',
      y = 'filtered_cnefe_cep',
      output_tb = 'output_caso_09',
      key_cols <- cols_09,
      precision = 9L
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
  inputs <- list(estado, municipio, bairro)
  all_required_inputs <- !any(sapply(inputs, is.null))

  if(all_required_inputs){

      # delete cases where localidade is missing
      delete_null_localidade <- function(tb){
        query_delete_localidade_from_input <-
        sprintf('DELETE FROM "%s" WHERE "localidade" IS NOT NULL;', tb)
        DBI::dbExecute(con, query_delete_localidade_from_input)
        # DBI::dbGetQuery(con, 'SELECT COUNT(*) FROM "input_padrao_db"')
        }
      delete_null_localidade('input_padrao_db')
      delete_null_localidade('filtered_cnefe_cep')


      # narrow down search scope to localidade
      query_narrow_localidades <-
      'DELETE FROM "filtered_cnefe_cep"
      WHERE "localidade" NOT IN (SELECT DISTINCT "localidade" FROM "input_padrao_db");'
      DBI::dbExecute(con, query_narrow_localidades)
      # DBI::dbGetQuery(con, 'SELECT COUNT(*) FROM "filtered_cnefe_cep"')


      temp_n <- match_aggregated_cases(
        con,
        x = 'input_padrao_db',
        y = 'filtered_cnefe_cep',
        output_tb = 'output_caso_10',
        key_cols <- cols_10,
        precision = 10L
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
        base::close(pb)
      }
  }

  ## CASE 11 --------------------------------------------------------------------
  # TO DO
  # add to package a table with the coordinates of geobr::read_municipal_seat()
  # and perform lookup
        # temp_n <- match_aggregated_cases(
        #   con,
        #   x = 'input_padrao_db',
        #   y = 'filtered_cnefe_cep',
        #   output_tb = 'output_caso_11',
        #   key_cols <- cols_11,
        #   precision = 11L
        # )
        #
        # # UPDATE input_padrao_db: Remove observations found in previous step
        # update_input_db(
        #   con,
        #   update_tb = 'input_padrao_db',
        #   reference_tb = 'output_caso_11'
        # )
        #
        # # update progress bar
        # if (isTRUE(showProgress)) {
        #   ndone <- ndone + temp_n
        #   utils::setTxtProgressBar(pb, ndone)
        #
        #   base::close(pb)
        # }


  ## CASE 999 --------------------------------------------------------------------
  # TO DO
  # WHAT SHOULD BE DONE FOR CASES NOT FOUND ?
  # AND THEIR EFFECT ON THE PROGRESS BAR


  # DBI::dbReadTable(con, 'input_padrao_db')
  # DBI::dbReadTable(con, 'output_caso_01')

  # DBI::dbRemoveTable(con, 'output_caso_01')
  # DBI::dbRemoveTable(con, 'output_caso_02')
  # DBI::dbRemoveTable(con, 'output_caso_03')
  # DBI::dbRemoveTable(con, 'output_caso_04')
  # DBI::dbRemoveTable(con, 'output_caso_05')
  # DBI::dbRemoveTable(con, 'output_caso_06')
  # DBI::dbRemoveTable(con, 'output_caso_07')
  # DBI::dbRemoveTable(con, 'output_caso_08')
  # DBI::dbRemoveTable(con, 'output_caso_09')
  # DBI::dbRemoveTable(con, 'output_caso_10')
  # DBI::dbRemoveTable(con, 'output_caso_11')




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
    ifelse( DBI::dbExistsTable(con, 'output_caso_10'), 'output_caso_10', 'empty')
  )
  all_output_tbs <- all_output_tbs[!grepl('empty', all_output_tbs)]

  # output with only ID and geocode columns
  if (isTRUE(output_simple)) {

    # build query
    outout_query <- paste(
      paste0("SELECT ", paste0('*', collapse = ', '), " FROM ", all_output_tbs),
      collapse = " UNION ALL ")

    output_deterministic <- DBI::dbGetQuery(con, outout_query)

    # output_deterministic <- lapply(X = all_output_tbs,
    #        FUN = function(x){
    #          DBI::dbGetQuery(con, sprintf("SELECT * FROM %s", x))
    #        }
    #        )
    # output_deterministic <- data.table::rbindlist(output_deterministic)


  }

  # output with all original columns
  if (isFALSE(output_simple)){

    # Convert input data frame to DuckDB table
    duckdb::dbWriteTable(con, "output_db", input_padrao,
                         temporary = TRUE, overwrite=TRUE)

      # merge_results(con,
      #               x='output_caso_01',
      #               y='output_db',
      #               key_column='ID',
      #               select_columns = "*")

    # Combine all cases into one data.table
    output_deterministic <- lapply(
      X = all_output_tbs,
      FUN = function(x){
        merge_results(
          con,
          x = x,
          y='output_db',
          key_column='ID',
          select_columns = "*")
      }
    )

    output_deterministic <- data.table::rbindlist(output_deterministic)
  }

  # Disconnect from DuckDB when done
  duckdb::duckdb_unregister_arrow(con, 'cnefe')
  duckdb::dbDisconnect(con, shutdown=TRUE)
  gc()

  # Return the result
  return(output_deterministic)

  #   # NEXT STEPS
  #   - optimize disk and parallel operations in duckdb
  #   - cases that matche first 7 digits of CEP
  #   - exceptional cases (no info on municipio input)
  #   - join probabilistico
  #   - interpolar numeros na mesma rua

}

