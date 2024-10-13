#' Download microdata of population records from Brazil's census
#'
#' @description
#' Download microdata of population records from Brazil's census. Data collected
#' in the sample component of the questionnaire.
#'
#' @template year
#' @template columns
#' @template add_labels
#' @template as_data_frame
#' @template showProgress
#' @param ncores Number of cores to be used in parallel execution. Defaults to
#'        the number of available cores minus 1.
#' @template cache
#'
#' @return An arrow `Dataset` or a `"data.frame"` object.
#' @export
#' @family Microdata
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
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
                         ncores = NULL,
                         cache = TRUE){

  # check input
  checkmate::assert_data_frame(input_table)
  checkmate::assert_number(ncores, null.ok = TRUE)

  # correspondence of column names
  campos <- enderecopadrao::correspondencia_campos(
    logradouro = {logradouro},
    numero = {numero},
    complemento = {complemento},
    cep = {cep},
    bairro = {bairro},
    municipio = {municipio},
    estado = {estado}
  )

  # padroniza input do usuario
  input_padrao <- enderecopadrao::padronizar_enderecos(
    enderecos = input_table,
    campos_do_endereco = campos
  )

  # 6666666666666666666666666666
  input_padrao <- cbind(input_table['ID'], input_padrao) # REMOVER quando EP manter ID
  input_padrao$estado <- input_table$nm_uf # REMOVER quando EP concerntar estados

  # create db connection
  db_path <- fs::file_temp(ext = '.duckdb')
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir=db_path)       # run on disk ?
  # con <- duckdb::dbConnect(duckdb::duckdb(), dbdir=":memory:")  # run on memory

  ## configure db connection
    # Set Number of cores for parallel operation
    if (is.null(ncores)) {
      ncores <- parallel::detectCores()
      ncores <- ncores-1
      }
    DBI::dbExecute(con, sprintf("PRAGMA threads=%s", ncores))

    # Set Memory limit
    # TODO

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                       temporary = TRUE, overwrite=TRUE)

  # add abbrev state
  add_abbrev_state_col(con, update_tb = "input_padrao_db")

  # determine states present in the input
  query <- "SELECT DISTINCT abbrev_state FROM input_padrao_db"
  input_states <- DBI::dbGetQuery(con, query)[[1]]

  ### 1 download cnefe no cache
  download_success <- download_cnefe(input_states)

  # check if download worked
  if (isFALSE(download_success)) { return(invisible(NULL)) }

  # Load CNEFE data and write to DuckDB
  cnefe <- arrow_open_dataset(geocodebr_env$cache_dir)
  duckdb::duckdb_register_arrow(con, "cnefe", cnefe)

  ## more than 2x SLOWER
  # dir <- paste0(geocodebr_env$cache_dir, "/**/*.parquet")
  # DBI::dbExecute(con,
  #           sprintf("CREATE TABLE cnefe AS SELECT * FROM parquet_scan('%s')",
  #                   dir))

  ##  DBI::dbRemoveTable(con, 'cnefe')


  # Narrow scope to municipalities and zip codes
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


  ### START DETERMINISTIC MATCHING

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

  # CASE 1 --------------------------------------------------------------------
  match_case(
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

  # CASE 2 --------------------------------------------------------------------
  match_case(
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

  # CASE 3 --------------------------------------------------------------------
  match_case(
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


  # CASE 4  --------------------------------------------------------------------
  match_case(
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

  # CASE 5  --------------------------------------------------------------------
  match_case(
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


  # CASE 6 --------------------------------------------------------------------
  match_case(
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


  # CASE 7 --------------------------------------------------------------------
  match_case(
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


  # CASE 8 --------------------------------------------------------------------
  match_case(
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


  # CASE 9 --------------------------------------------------------------------
  match_case(
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


  # CASE 10 --------------------------------------------------------------------
  match_case(
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

  # CASE 11 --------------------------------------------------------------------
  match_case(
    con,
    x = 'input_padrao_db',
    y = 'filtered_cnefe_cep',
    output_tb = 'output_caso_11',
    key_cols <- cols_11,
    precision = 11L
  )

  # UPDATE input_padrao_db: Remove observations found in previous step
  update_input_db(
    con,
    update_tb = 'input_padrao_db',
    reference_tb = 'output_caso_11'
  )


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


 # THIS NEEDS TO BE IMPROVED

  # prepare output --------------------------------------------------------------------

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "output_db", input_padrao,
                       temporary = TRUE, overwrite=TRUE)

  # merge_results(con,
  #               x='output_db',
  #               y='output_caso_01',
  #               key_column='ID',
  #               select_columns = c('lon', 'lat', 'precision')
  # )

  # TO DO: replace code below with
  ######   MERGE columns back into db
  ######   THEN return

  # Combine results
  output_caso_01 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_01")
  output_caso_02 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_02")
  output_caso_03 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_03")
  output_caso_04 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_04")
  output_caso_05 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_05")
  output_caso_06 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_06")
  output_caso_07 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_07")
  output_caso_08 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_08")
  output_caso_09 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_09")
  output_caso_10 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_10")
  output_caso_11 <- DBI::dbGetQuery(con, "SELECT * FROM output_caso_11")

  # Combine all cases into one data.table
  output_deterministic <- data.table::rbindlist(
    list(output_caso_01,
         output_caso_02,
         output_caso_03,
         output_caso_04,
         output_caso_05,
         output_caso_06,
         output_caso_07,
         output_caso_08,
         output_caso_09,
         output_caso_10,
         output_caso_11
    ),
    fill = TRUE
  )

  # Disconnect from DuckDB when done
  duckdb::dbDisconnect(con)

  # Return the result
  return(output_deterministic)



  #   # NEXT STEPS
  #   - optimize disk and parallel operations in duckdb
  #   - cases that matche first 7 digits of CEP
  #   - exceptional cases (no info on municipio input)
  #   - join probabilistico
  #   - interpolar numeros na mesma rua

}

