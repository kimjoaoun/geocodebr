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
geocode_duck <- function(input_table,
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


  # add abbrev state
  .datatable.aware = TRUE
  data.table::setDT(input_padrao)
  input_padrao[, abbrev_state := data.table::fcase(
    estado == "RONDONIA", "RO",
    estado == "ACRE", "AC",
    estado == "AMAZONAS", "AM",
    estado == "RORAIMA", "RR",
    estado == "PARA", "PA",
    estado == "AMAPA", "AP",
    estado == "TOCANTINS", "TO",
    estado == "MARANHAO", "MA",
    estado == "PIAUI", "PI",
    estado == "CEARA", "CE",
    estado == "RIO GRANDE DO NORTE", "RN",
    estado == "PARAIBA", "PB",
    estado == "PERNAMBUCO", "PE",
    estado == "ALAGOAS", "AL",
    estado == "SERGIPE", "SE",
    estado == "BAHIA", "BA",
    estado == "MINAS GERAIS", "MG",
    estado == "ESPIRITO SANTO", "ES",
    estado == "RIO DE JANEIRO", "RJ",
    estado == "SAO PAULO", "SP",
    estado == "PARANA", "PR",
    estado == "SANTA CATARINA", "SC",
    estado == "RIO GRANDE DO SUL", "RS",
    estado == "MATO GROSSO DO SUL", "MS",
    estado == "MATO GROSSO", "MT",
    estado == "GOIAS", "GO",
    estado == "DISTRITO FEDERAL", "DF"
  )]


  # determine states present in the input
  input_states <- unique(input_padrao$abbrev_state)

  ### 1 download cnefe no cache
  download_success <- download_cnefe(input_states)

  # check if download worked
  if (isFALSE(download_success)) { return(invisible(NULL)) }

  # Step 7: Open a connection to DuckDB and create a temporary database
  con <- duckdb::dbConnect(duckdb::duckdb())

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_padrao", input_padrao,
                       temporary = TRUE, overwrite=TRUE)

  # Step 8: Load CNEFE data and write to DuckDB
  cnefe <- arrow_open_dataset(geocodebr_env$cache_dir)
  duckdb::duckdb_register_arrow(con, "cnefe", cnefe)

        # # narrow scope of search to states
        # # Step 9: Filter CNEFE data to the states present in the input
        # # This creates a temporary table "filtered_cnefe" inside DuckDB
        # input_states <- unique(input_padrao$abbrev_state)
        # query_states <- sprintf("
        # CREATE TEMPORARY TABLE filtered_cnefe_state AS
        # SELECT * FROM cnefe
        # WHERE abbrev_state IN ('%s')",
        #                         paste(input_states, collapse = "', '")
        #                         )
        # DBI::dbExecute(con, query_states)

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

  #   - case 01: match municipio, logradouro, numero, cep, bairro
  #   - case 02: match municipio, logradouro, numero, cep
  #   - case 03: match municipio, logradouro, cep, bairro
  #   - case 04: match municipio, logradouro, numero
  #   - case 05: match municipio, logradouro, cep
  # TODO  - case 06: match municipio, logradouro, bairro
  # TODO  - case 07: match municipio, logradouro
  # TODO  - case 08: match municipio, cep, bairro
  # TODO  - case 09: match municipio, cep
  # TODO  - case 10: match municipio, bairro


  # Case 1: Match by estado, municipio, logradouro, numero, cep, bairro
  cols_1 <- c("estado", "municipio", "logradouro", "numero", "cep", "bairro")
  cols_select1 <- paste("input_padrao.", c('ID', cols_1), sep = "", collapse = ", ")
  cols_group1 <- paste("input_padrao.", cols_1, sep = "", collapse = ", ")

  query_case_1 <- sprintf("
  CREATE TEMPORARY TABLE output_caso_1 AS
  SELECT %s, AVG(filtered_cnefe_cep.lon) AS lon, AVG(filtered_cnefe_cep.lat) AS lat
  FROM input_padrao
  LEFT JOIN filtered_cnefe_cep
  ON input_padrao.estado = filtered_cnefe_cep.estado
    AND input_padrao.municipio = filtered_cnefe_cep.municipio
    AND input_padrao.logradouro = filtered_cnefe_cep.logradouro
    AND input_padrao.numero = filtered_cnefe_cep.numero
    AND input_padrao.cep = filtered_cnefe_cep.cep
    AND input_padrao.bairro = filtered_cnefe_cep.bairro
  WHERE filtered_cnefe_cep.lon IS NOT NULL
  GROUP BY input_padrao.ID, %s",
                          cols_select1, cols_group1)
  DBI::dbExecute(con, query_case_1)

  # DBI::dbReadTable(con, 'input_padrao')
  # DBI::dbReadTable(con, 'output_caso_1')
# DBI::dbRemoveTable(con, 'output_caso_1')
# DBI::dbRemoveTable(con, 'output_caso_2')
# DBI::dbRemoveTable(con, 'output_caso_3')
# DBI::dbRemoveTable(con, 'output_caso_4')

  # UPDATE input_padrao: Remove observations found in previous step
  query_remove_matched <- "
  DELETE FROM input_padrao
  WHERE ID IN (SELECT ID FROM output_caso_1)"
  DBI::dbExecute(con, query_remove_matched)


  # Case 2: Match by estado, municipio, logradouro, numero, cep
  cols_2 <- c("estado", "municipio", "logradouro", "numero", "cep")
  cols_select2 <- paste("input_padrao.", c('ID', cols_2), sep = "", collapse = ", ")
  cols_group2 <- paste("input_padrao.", cols_2, sep = "", collapse = ", ")

  query_case_2 <- sprintf("
  CREATE TEMPORARY TABLE output_caso_2 AS
  SELECT %s, AVG(filtered_cnefe_cep.lon) AS lon, AVG(filtered_cnefe_cep.lat) AS lat
  FROM input_padrao
  LEFT JOIN filtered_cnefe_cep
  ON input_padrao.estado = filtered_cnefe_cep.estado
    AND input_padrao.municipio = filtered_cnefe_cep.municipio
    AND input_padrao.logradouro = filtered_cnefe_cep.logradouro
    AND input_padrao.numero = filtered_cnefe_cep.numero
    AND input_padrao.cep = filtered_cnefe_cep.cep
  WHERE filtered_cnefe_cep.lon IS NOT NULL
  GROUP BY input_padrao.ID, %s",
                          cols_select2, cols_group2)
  DBI::dbExecute(con, query_case_2)

  # UPDATE input_padrao: Remove observations found in previous step
  query_remove_matched <- "
  DELETE FROM input_padrao
  WHERE ID IN (SELECT ID FROM output_caso_2)"
  DBI::dbExecute(con, query_remove_matched)

  # Case 3: Match by estado, municipio, logradouro, cep, bairro
  cols_3 <- c("estado", "municipio", "logradouro", "cep", "bairro")
  cols_select3 <- paste("input_padrao.", c('ID', cols_3), sep = "", collapse = ", ")
  cols_group3 <- paste("input_padrao.", cols_3, sep = "", collapse = ", ")

  query_case_3 <- sprintf("
  CREATE TEMPORARY TABLE output_caso_3 AS
  SELECT %s, AVG(filtered_cnefe_cep.lon) AS lon, AVG(filtered_cnefe_cep.lat) AS lat
  FROM input_padrao
  LEFT JOIN filtered_cnefe_cep
  ON input_padrao.estado = filtered_cnefe_cep.estado
    AND input_padrao.municipio = filtered_cnefe_cep.municipio
    AND input_padrao.logradouro = filtered_cnefe_cep.logradouro
    AND input_padrao.cep = filtered_cnefe_cep.cep
    AND input_padrao.bairro = filtered_cnefe_cep.bairro
  WHERE filtered_cnefe_cep.lon IS NOT NULL
  GROUP BY input_padrao.ID, %s",
                          cols_select3, cols_group3)
  DBI::dbExecute(con, query_case_3)


  # UPDATE input_padrao: Remove observations found in previous step
  query_remove_matched <- "
  DELETE FROM input_padrao
  WHERE ID IN (SELECT ID FROM output_caso_3)"
  DBI::dbExecute(con, query_remove_matched)


  # Case 4: Match by estado, municipio, logradouro, numero
  cols_4 <- c("estado", "municipio", "logradouro", "numero")
  cols_select4 <- paste("input_padrao.", c('ID', cols_4), sep = "", collapse = ", ")
  cols_group4 <- paste("input_padrao.", cols_4, sep = "", collapse = ", ")

  query_case_4 <- sprintf("
  CREATE TEMPORARY TABLE output_caso_4 AS
  SELECT %s, AVG(filtered_cnefe_cep.lon) AS lon, AVG(filtered_cnefe_cep.lat) AS lat
  FROM input_padrao
  LEFT JOIN filtered_cnefe_cep
  ON input_padrao.estado = filtered_cnefe_cep.estado
    AND input_padrao.municipio = filtered_cnefe_cep.municipio
    AND input_padrao.logradouro = filtered_cnefe_cep.logradouro
    AND input_padrao.numero = filtered_cnefe_cep.numero
  WHERE filtered_cnefe_cep.lon IS NOT NULL
  GROUP BY input_padrao.ID, %s",
                          cols_select4, cols_group4)
  DBI::dbExecute(con, query_case_4)

  # UPDATE input_padrao: Remove observations found in previous step
  query_remove_matched <- "
  DELETE FROM input_padrao
  WHERE ID IN (SELECT ID FROM output_caso_4)"
  DBI::dbExecute(con, query_remove_matched)



  # Case 5: Match by estado, municipio, logradouro, cep
  cols_5 <- c("estado", "municipio", "logradouro", "cep")
  cols_select5 <- paste("input_padrao.", c('ID', cols_5), sep = "", collapse = ", ")
  cols_group5 <- paste("input_padrao.", cols_5, sep = "", collapse = ", ")

  query_case_5 <- sprintf("
  CREATE TEMPORARY TABLE output_caso_5 AS
  SELECT %s, AVG(filtered_cnefe_cep.lon) AS lon, AVG(filtered_cnefe_cep.lat) AS lat
  FROM input_padrao
  LEFT JOIN filtered_cnefe_cep
  ON input_padrao.estado = filtered_cnefe_cep.estado
    AND input_padrao.municipio = filtered_cnefe_cep.municipio
    AND input_padrao.logradouro = filtered_cnefe_cep.logradouro
    AND input_padrao.cep = filtered_cnefe_cep.cep
  WHERE filtered_cnefe_cep.lon IS NOT NULL
  GROUP BY input_padrao.ID, %s",
                          cols_select5, cols_group5)
  DBI::dbExecute(con, query_case_5)

  # UPDATE input_padrao: Remove observations found in previous step
  query_remove_matched <- "
  DELETE FROM input_padrao
  WHERE ID IN (SELECT ID FROM output_caso_5)"
  DBI::dbExecute(con, query_remove_matched)



  # Combine results
  output_caso_1 <- DBI::dbGetQuery(con, "SELECT *, 1 AS accuracy_g FROM output_caso_1")
  output_caso_2 <- DBI::dbGetQuery(con, "SELECT *, 2 AS accuracy_g FROM output_caso_2")
  output_caso_3 <- DBI::dbGetQuery(con, "SELECT *, 3 AS accuracy_g FROM output_caso_3")
  output_caso_4 <- DBI::dbGetQuery(con, "SELECT *, 4 AS accuracy_g FROM output_caso_4")
  output_caso_5 <- DBI::dbGetQuery(con, "SELECT *, 5 AS accuracy_g FROM output_caso_5")

  # Combine all cases into one data.table
  output_deterministic <- data.table::rbindlist(
    list(output_caso_1, output_caso_2, output_caso_3, output_caso_4, output_caso_5),
    fill = TRUE
  )

  # Disconnect from DuckDB when done
  duckdb::dbDisconnect(con)

  # Return the result
  return(output_deterministic)



  #   # NEXT STEPS
  #   - join probabilistico
  #   - interpolar numeros na mesma rua

}

