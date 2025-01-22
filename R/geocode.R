#' Geolocaliza endereços no Brasil
#'
#' Geocodifica endereços brasileiros com base nos dados do CNEFE. Os endereços
#' de input devem ser passados como um `data.frame`, no qual cada coluna
#' descreve um campo do endereço (logradouro, número, cep, etc). O resuldos dos
#' endereços geolocalizados podem seguir diferentes níveis de precisão. Consulte
#' abaixo a seção "Detalhes" para mais informações. As coordenadas de output
#' utilizam o sistema de referência geodésico "SIRGAS2000", CRS(4674).
#'
#' @param enderecos Um `data.frame`. Os endereços a serem geolocalizados. Cada
#'    coluna deve representar um campo do endereço.
#' @param campos_endereco Um vetor de caracteres. A correspondência entre cada
#'    campo de endereço e o nome da coluna que o descreve na tabela `enderecos`.
#'    A função [listar_campos()] auxilia na criação deste vetor e realiza
#'    algumas verificações nos dados de entrada. Campos de endereço passados
#'    como `NULL` serão ignorados, e a função deve receber pelo menos um campo
#'    não nulo, além  dos campos `"estado"` e `"municipio"`, que são
#'    obrigatórios. Note que o campo  `"localidade"` é equivalente a 'bairro'.
#' @param resultado_completo Lógico. Indica se o output deve incluir colunas
#'    adicionais, como o endereço encontrado de referência. Por padrão, é `FALSE`.
#' @template verboso
#' @template cache
#' @template n_cores
#'
#' @return Retorna o `data.frame` de input `enderecos` adicionado das colunas de
#'   latitude (`lat`) e longitude (`lon`), bem como as colunas (`precisao` e
#'   `tipo_resultado`) que indicam o nível de precisão e o tipo de resultado.
#'
#' @template precision_section
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#'
#' # ler amostra de dados
#' data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
#' input_df <- read.csv(data_path)
#'
#' fields <- geocodebr::listar_campos(
#'   logradouro = "nm_logradouro",
#'   numero = "Numero",
#'   cep = "Cep",
#'   localidade = "Bairro",
#'   municipio = "nm_municipio",
#'   estado = "nm_uf"
#' )
#'
#' df <- geocodebr::geocode(
#'   enderecos = input_df,
#'   campos_endereco = fields,
#'   verboso = FALSE
#'   )
#'
#' head(df)
#'
#' @export
geocode <- function(enderecos,
                    campos_endereco = listar_campos(),
                    resultado_completo = FALSE,
                    verboso = TRUE,
                    cache = TRUE,
                    n_cores = 1 ){

  # check input
  checkmate::assert_data_frame(enderecos)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)
  checkmate::assert_logical(verboso, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  checkmate::assert_logical(resultado_completo, any.missing = FALSE, len = 1)
  campos_endereco <- assert_and_assign_address_fields(
    campos_endereco,
    enderecos
  )

  # normalize input data -------------------------------------------------------

  # standardizing the addresses table to increase the chances of finding a match
  # in the CNEFE data

  if (verboso) message_standardizing_addresses()

  input_padrao <- enderecobr::padronizar_enderecos(
    enderecos,
    campos_do_endereco = enderecobr::correspondencia_campos(
      logradouro = campos_endereco[["logradouro"]],
      numero = campos_endereco[["numero"]],
      cep = campos_endereco[["cep"]],
      bairro = campos_endereco[["localidade"]],
      municipio = campos_endereco[["municipio"]],
      estado = campos_endereco[["estado"]]
    ),
    formato_estados = "sigla",
    formato_numeros = 'integer'
  )


  # keep and rename colunms of input_padrao to use the
  # same column names used in cnefe data set
  data.table::setDT(input_padrao)
  cols_to_keep <- names(input_padrao)[ names(input_padrao) %like% '_padr']
  input_padrao <- input_padrao[, .SD, .SDcols = c(cols_to_keep)]
  names(input_padrao) <- c(gsub("_padr", "", names(input_padrao)))

  if ('logradouro' %in% names(input_padrao)) {
  data.table::setnames(
    x = input_padrao, old = 'logradouro', new = 'logradouro_sem_numero'
    )
  }

  if ('bairro' %in% names(input_padrao)) {
    data.table::setnames(
      x = input_padrao, old = 'bairro', new = 'localidade'
    )
  }

  # create temp id
  input_padrao[, tempidgeocodebr := 1:nrow(input_padrao) ]
  data.table::setDT(enderecos)[, tempidgeocodebr := 1:nrow(input_padrao) ]

  # # sort input data
  # input_padrao <- input_padrao[order(estado, municipio, logradouro_sem_numero, numero, cep, localidade)]


  # downloading cnefe
  cnefe_dir <- download_cnefe(
    verboso = verboso,
    cache = cache
  )

  # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)

  # Convert input data frame to DuckDB table
  duckdb::dbWriteTable(con, "input_padrao_db", input_padrao,
                       overwrite = TRUE, temporary = TRUE
                       )

  # create an empty output table that will be populated -----------------------------------------------

  additional_cols <- ""
  if (isTRUE(resultado_completo)) {
    additional_cols <- glue::glue(
    ", endereco_encontrado VARCHAR, logradouro_encontrado VARCHAR,
    numero_encontrado VARCHAR, localidade_encontrada VARCHAR,
    cep_encontrado VARCHAR, municipio_encontrado VARCHAR, estado_encontrado VARCHAR"
    )
  }

  query_create_empty_output_db <- glue::glue(
    "CREATE TABLE output_db (
     tempidgeocodebr INTEGER,
     lat NUMERIC,
     lon NUMERIC,
     tipo_resultado VARCHAR {additional_cols});"
    )

  DBI::dbExecute(con, query_create_empty_output_db)


  # START MATCHING -----------------------------------------------

  # start progress bar
  if (verboso) {
    prog <- create_progress_bar(input_padrao)
    message_looking_for_matches()
  }

  n_rows <- nrow(input_padrao)
  matched_rows <- 0

  # start matching
  for (case in all_possible_match_types ) {

    relevant_cols <- get_relevant_cols_arrow(case)

    if (verboso) update_progress_bar(matched_rows, case)


    if (all(relevant_cols %in% names(input_padrao))) {

      # select match function
      match_fun <- ifelse(case %in% number_interpolation_types, match_weighted_cases, match_cases)

      n_rows_affected <- match_fun(
        con,
        x = 'input_padrao_db',
        y = 'filtered_cnefe', # keep this for now
        output_tb = "output_db",
        key_cols = relevant_cols,
        match_type = case,
        resultado_completo = resultado_completo
      )

      matched_rows <- matched_rows + n_rows_affected

      # leave the loop early if we find all addresses before covering all cases
      if (matched_rows == n_rows) break
    }

  }

  if (verboso) finish_progress_bar(matched_rows)


  # add precision column
  add_precision_col(con, update_tb = 'output_db')

  # output with all original columns
  duckdb::dbWriteTable(con, "input_db", enderecos,
                       temporary = TRUE, overwrite=TRUE)

  x_columns <- names(enderecos)

  output_deterministic <- merge_results(
    con,
    x='input_db',
    y='output_db',
    key_column='tempidgeocodebr',
    select_columns = x_columns,
    resultado_completo = resultado_completo
  )

  # Disconnect from DuckDB when done
  duckdb::dbDisconnect(con)

  # Return the result
  return(output_deterministic)
}
