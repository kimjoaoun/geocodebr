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
                         cache = TRUE){

  # check input
  checkmate::assert_data_frame(input_table)

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

  input_padrao <- cbind(input_table['ID'], input_padrao) # REMOVER quando EP manter ID

  # Step 7: Open a connection to DuckDB and create a temporary database
  con <- duckdb::dbConnect(duckdb::duckdb())

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                       temporary = TRUE, overwrite=TRUE)

  # add abbrev state
  add_abbrev_state_col(con, update_tb = "input_padrao_db")




  # determine states present in the input
  input_states <- unique(input_padrao$abbrev_state)

  ### 1 download cnefe no cache
  download_success <- download_cnefe(input_states)

  # check if download worked
  if (isFALSE(download_success)) { return(invisible(NULL)) }



  # Step 8: Load CNEFE data and write to DuckDB
  cnefe <- arrow_open_dataset(geocodebr_env$cache_dir)
  duckdb::duckdb_register_arrow(con, "cnefe", cnefe)


  # Step 10: Narrow scope to municipalities and zip codes (still using DuckDB without loading to R)
  input_ceps <- unique(input_padrao$cep)
  input_municipio <- unique(input_padrao$municipio)
  query_ceps_municipios <- sprintf("
  CREATE TEMPORARY TABLE filtered_cnefe_cep AS
  SELECT * FROM cnefe
  WHERE cep IN ('%s') OR municipio IN ('%s')",
                                   paste(input_ceps, collapse = "', '"),
                                   paste(input_municipio, collapse = "', '")
  )
  DBI::dbExecute(con, query_ceps_municipios)
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
    key_cols <- cols_01
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
    key_cols <- cols_02
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
    key_cols <- cols_03
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
    key_cols <- cols_04
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
    key_cols <- cols_05
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
    key_cols <- cols_06
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
    key_cols <- cols_07
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
    key_cols <- cols_08
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
    key_cols <- cols_09
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
    key_cols <- cols_10
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
    key_cols <- cols_11
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


  # Combine results
  output_caso_01 <- DBI::dbGetQuery(con, "SELECT *,  1 AS accuracy_g FROM output_caso_01")
  output_caso_02 <- DBI::dbGetQuery(con, "SELECT *,  2 AS accuracy_g FROM output_caso_02")
  output_caso_03 <- DBI::dbGetQuery(con, "SELECT *,  3 AS accuracy_g FROM output_caso_03")
  output_caso_04 <- DBI::dbGetQuery(con, "SELECT *,  4 AS accuracy_g FROM output_caso_04")
  output_caso_05 <- DBI::dbGetQuery(con, "SELECT *,  5 AS accuracy_g FROM output_caso_05")
  output_caso_06 <- DBI::dbGetQuery(con, "SELECT *,  6 AS accuracy_g FROM output_caso_06")
  output_caso_07 <- DBI::dbGetQuery(con, "SELECT *,  7 AS accuracy_g FROM output_caso_07")
  output_caso_08 <- DBI::dbGetQuery(con, "SELECT *,  8 AS accuracy_g FROM output_caso_08")
  output_caso_09 <- DBI::dbGetQuery(con, "SELECT *,  9 AS accuracy_g FROM output_caso_09")
  output_caso_10 <- DBI::dbGetQuery(con, "SELECT *, 10 AS accuracy_g FROM output_caso_10")
  output_caso_11 <- DBI::dbGetQuery(con, "SELECT *, 11 AS accuracy_g FROM output_caso_11")

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
  #   - matches first 7 digits of CEP
  #   - exceptional cases (no municipio input)
  #   - join probabilistico
  #   - interpolar numeros na mesma rua

}

