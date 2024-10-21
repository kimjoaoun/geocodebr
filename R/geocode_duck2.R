#' Download microdata of population records from Brazil's census
#'
#' @description
#' Download microdata of population records from Brazil's census. Data collected
#' in the sample component of the questionnaire.
#'
#' @template year
#' @template columns
#' @template add_labels
#' @template showProgress
#' @param output_simple Logic. Defaults to `TRUE`
#' @param ncores Number of cores to be used in parallel execution. Defaults to
#'        the number of available cores minus 1.
#' @template cache
#'
#' @return An arrow `Dataset` or a `"data.frame"` object.
#' @export
#' @family Microdata
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' library(geocodebr)
#'
#' # open input data
#' data_path <- system.file("extdata/sample_1.csv", package = "geocodebr")
#' input_df <- read.csv(data_path)
#'
#' df <- geocodebr::geocode_duck(
#'   input_table = input_df,
#'   logradouro = "nm_logradouro",
#'   numero = "Numero",
#'   complemento = "Complemento",
#'   cep = "Cep",
#'   bairro = "Bairro",
#'   municipio = "nm_municipio",
#'   estado = "nm_uf"
#'   )
#'
geocode_duck2 <- function(input_table,
                          logradouro = NULL,
                          numero = NULL,
                          complemento = NULL,
                          cep = NULL,
                          bairro = NULL,
                          municipio = NULL,
                          estado = NULL,
                          showProgress = TRUE,
                          output_simple = TRUE,
                          ncores = NULL,
                          cache = TRUE){

  # check input
  checkmate::assert_data_frame(input_table)
  checkmate::assert_logical(showProgress)
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
  campos <- enderecopadrao::correspondencia_campos(
    logradouro = logradouro,
    numero = numero,
    complemento = complemento,
    cep = cep,
    bairro = bairro,
    municipio = municipio,
    estado = estado
  )

  # padroniza input do usuario
  input_padrao <- enderecopadrao::padronizar_enderecos(
    enderecos = input_table,
    campos_do_endereco = campos
  )

  # 6666666666666666666666666666
  # REMOVER quando atualizar EP
  input_padrao <- cbind(input_table['ID'], input_padrao)
  input_padrao$estado <- input_df$nm_uf


  # create db connection -------------------------------------------------------


  # create db connection
  # db_path <- fs::file_temp(ext = '.duckdb')
  # con <- duckdb::dbConnect(duckdb::duckdb(), dbdir=db_path)       # run on disk ?
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir=":memory:")  # run on memory

  ## configure db connection
    # Set Number of cores for parallel operation
    if (is.null(ncores)) {
      ncores <- parallel::detectCores()
      ncores <- ncores-1
      if (ncores<1) {ncores <- 1}
    }

    DBI::dbExecute(con, sprintf("PRAGMA threads=%s;", ncores))

    # Set Memory limit
    # TODO
    # DBI::dbExecute(con, "SET memory_limit = '20GB'")


    # add input and cnefe data sets to db --------------------------------------

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                       temporary = TRUE, overwrite=TRUE)

  # add abbrev state
  add_abbrev_state_col(con, update_tb = "input_padrao_db")

  # determine states present in the input
  query <- "SELECT DISTINCT abbrev_state FROM input_padrao_db"
  input_states <- DBI::dbGetQuery(con, query)[[1]]

  # download cnefe
  download_success <- download_cnefe(
    abbrev_state = input_states,
    showProgress = showProgress,
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
  input_municipio <- unique(input_padrao$municipio)
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

  # - case 01: match municipio, logradouro, numero, cep, bairro
  # - case 02: match municipio, logradouro, numero, cep
  # - case 03: match municipio, logradouro, cep, bairro
  # - case 04: match municipio, logradouro, numero
  # - case 05: match municipio, logradouro, cep
  # - case 06: match municipio, logradouro, bairro
  # - case 07: match municipio, logradouro
  # - case 08: match municipio, cep, bairro
  # - case 09: match municipio, cep
  # - case 10: match municipio, bairro
  # - case 11: match municipio


  # key columns
  cols_01 <- c("estado", "municipio", "logradouro", "numero", "cep", "bairro")
  cols_02 <- c("estado", "municipio", "logradouro", "numero", "cep")
  cols_03 <- c("estado", "municipio", "logradouro", "cep", "bairro")
  cols_04 <- c("estado", "municipio", "logradouro", "numero")
  cols_05 <- c("estado", "municipio", "logradouro", "cep")
  cols_06 <- c("estado", "municipio", "logradouro", "bairro")
  cols_07 <- c("estado", "municipio", "logradouro")
  cols_08 <- c("estado", "municipio", "cep", "bairro")
  cols_09 <- c("estado", "municipio", "cep")
  cols_10 <- c("estado", "municipio", "bairro")
  cols_11 <- c("estado", "municipio")

  # start progress bar
  if (isTRUE(showProgress)) {
    total_n <- nrow(input_df)
    pb <- txtProgressBar(min = 0, max = total_n, style = 3)
    }

  ## CASE 1 --------------------------------------------------------------------

  temp_n <- match_case(
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
  if (isTRUE(showProgress)) {
    ndone <- temp_n
    utils::setTxtProgressBar(pb, ndone)
    }

  # DBI::dbReadTable(con, 'output_caso_01')
  # DBI::dbRemoveTable(con, 'output_caso_01')

  ## CASE 2 --------------------------------------------------------------------
  temp_n <- match_case(
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
  if (isTRUE(showProgress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
  }

  ## CASE 3 --------------------------------------------------------------------
  temp_n <- match_case(
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
  if (isTRUE(showProgress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
  }

  ## CASE 4  --------------------------------------------------------------------
  temp_n <- match_case(
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
  if (isTRUE(showProgress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
  }

  ## CASE 5  --------------------------------------------------------------------
  temp_n <- match_case(
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
  if (isTRUE(showProgress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
  }

  ## CASE 6 --------------------------------------------------------------------
  temp_n <- match_case(
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
  if (isTRUE(showProgress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
  }

  ## CASE 7 --------------------------------------------------------------------
  temp_n <- match_case(
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
  if (isTRUE(showProgress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
  }

  ## CASE 8 --------------------------------------------------------------------
  temp_n <- match_case(
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
  if (isTRUE(showProgress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
  }

  ## CASE 9 --------------------------------------------------------------------
  temp_n <- match_case(
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
  if (isTRUE(showProgress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
  }

  ## CASE 10 --------------------------------------------------------------------


  # delete cases where Bairro is missing
  delete_null_bairro <- function(tb){
    query_delete_bairro_from_input <-
    sprintf('DELETE FROM "%s" WHERE "bairro" IS NOT NULL;', tb)
    DBI::dbExecute(con, query_delete_bairro_from_input)
    # DBI::dbGetQuery(con, 'SELECT COUNT(*) FROM "input_padrao_db"')
    }
  delete_null_bairro('input_padrao_db')
  delete_null_bairro('filtered_cnefe_cep')

  # narrow down search scope to Bairro
  query_narrow_bairros <-
  'DELETE FROM "filtered_cnefe_cep"
  WHERE "bairro" NOT IN (SELECT DISTINCT "bairro" FROM "input_padrao_db");'
  DBI::dbExecute(con, query_narrow_bairros)
  # DBI::dbGetQuery(con, 'SELECT COUNT(*) FROM "filtered_cnefe_cep"')


  temp_n <- match_case(
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
  if (isTRUE(showProgress)) {
    ndone <- ndone + temp_n
    utils::setTxtProgressBar(pb, ndone)
    base::close(pb)
  }

  ## CASE 11 --------------------------------------------------------------------
  # TO DO
  # add to package a table with the coordinates of geobr::read_municipal_seat()
  # and perform lookup
        # temp_n <- match_case(
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
    'output_caso_01',
    'output_caso_02',
    'output_caso_03',
    'output_caso_04',
    'output_caso_05',
    'output_caso_06',
    'output_caso_07',
    'output_caso_08',
    'output_caso_09',
    'output_caso_10'
    #, 'output_caso_11'
  )

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
  duckdb::dbDisconnect(con, shutdown=TRUE)

  # Return the result
  return(output_deterministic)

  #   # NEXT STEPS
  #   - optimize disk and parallel operations in duckdb
  #   - cases that matche first 7 digits of CEP
  #   - exceptional cases (no info on municipio input)
  #   - join probabilistico
  #   - interpolar numeros na mesma rua

}

