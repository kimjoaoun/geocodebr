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

  duckdb::duckdb_register(con, "standard_locations", standard_locations)

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




  return()







  # keep and rename colunms of input_padrao
  # keeping same column names used in our cnefe data set
  cols_padr <- grep("_padr", names(input_padrao_raw), value = TRUE)
  input_padrao <- input_padrao_raw[, .SD, .SDcols = c("ID", cols_padr)]
  names(input_padrao) <- c("ID", gsub("_padr", "", cols_padr))

  data.table::setnames(input_padrao, old = 'logradouro', new = 'logradouro_sem_numero')
  data.table::setnames(input_padrao, old = 'bairro', new = 'localidade')


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
