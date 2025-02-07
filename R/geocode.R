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
#'    A função [definir_campos()] auxilia na criação deste vetor e realiza
#'    algumas verificações nos dados de entrada. Campos de endereço passados
#'    como `NULL` serão ignorados, e a função deve receber pelo menos um campo
#'    não nulo, além  dos campos `"estado"` e `"municipio"`, que são
#'    obrigatórios. Note que o campo  `"localidade"` é equivalente a 'bairro'.
#' @param resultado_completo Lógico. Indica se o output deve incluir colunas
#'    adicionais, como o endereço encontrado de referência. Por padrão, é `FALSE`.
#' @param resolver_empates Lógico. Alguns resultados da geolocalização podem
#'        indicar diferentes coordenadas possíveis (e.g. duas ruas diferentes com
#'        o mesmo nome em uma mesma cidade). Esses casos são trados como 'empate'
#'        e o parâmetro `resolver_empates` indica se a função deve resolver esses
#'        empates automaticamente. Por padrão, é `FALSE`, e a função retorna
#'        apenas o caso mais provável.
#' @param resultado_sf Lógico. Indica se o resultado deve ser um objeto espacial
#'    da classe `sf`. Por padrão, é `FALSE`, e o resultado é um `data.frame` com
#'    as colunas `lat` e `lon`.
#'    adicionais, como o endereço encontrado de referência. Por padrão, é `FALSE`.
#' @template verboso
#' @template cache
#' @template n_cores
#'
#' @return Retorna o `data.frame` de input `enderecos` adicionado das colunas de
#'   latitude (`lat`) e longitude (`lon`), bem como as colunas (`precisao` e
#'   `tipo_resultado`) que indicam o nível de precisão e o tipo de resultado.
#'   Alternativamente, o resultado pode ser um objeto `sf`.
#'
#' @template precision_section
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#'
#' # ler amostra de dados
#' data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
#' input_df <- read.csv(data_path)
#'
#' fields <- geocodebr::definir_campos(
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
#'   resolver_empates = TRUE,
#'   verboso = FALSE
#'   )
#'
#' head(df)
#'
#' @export
geocode <- function(enderecos,
                    campos_endereco = definir_campos(),
                    resultado_completo = FALSE,
                    resolver_empates = FALSE,
                    resultado_sf = FALSE,
                    verboso = TRUE,
                    cache = TRUE,
                    n_cores = 1 ){

  # check input
  checkmate::assert_data_frame(enderecos)
  checkmate::assert_logical(resultado_completo, any.missing = FALSE, len = 1)
  checkmate::assert_logical(resultado_sf, any.missing = FALSE, len = 1)
  checkmate::assert_logical(verboso, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)
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

  if ('bairro' %in% names(input_padrao)) {
    data.table::setnames(
      x = input_padrao, old = 'bairro', new = 'localidade'
    )
  }

  # create temp id
  input_padrao[, tempidgeocodebr := 1:nrow(input_padrao) ]
  data.table::setDT(enderecos)[, tempidgeocodebr := 1:nrow(input_padrao) ]

  # # sort input data
  # input_padrao <- input_padrao[order(estado, municipio, logradouro, numero, cep, localidade)]

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
    ", logradouro_encontrado VARCHAR,
    numero_encontrado VARCHAR, localidade_encontrada VARCHAR,
    cep_encontrado VARCHAR, municipio_encontrado VARCHAR, estado_encontrado VARCHAR,
    similaridade_logradouro NUMERIC(5, 3)"
    )
  }

  query_create_empty_output_db <- glue::glue(
    "CREATE OR REPLACE TABLE output_db (
     tempidgeocodebr INTEGER,
     lat NUMERIC(8, 6),
     lon NUMERIC(8, 6),
     endereco_encontrado VARCHAR,
     tipo_resultado VARCHAR,
    contagem_cnefe INTEGER {additional_cols});"
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

  # start matching pi01
  for (match_type in all_possible_match_types ) {

    # get key cols
    key_cols <- get_key_cols(match_type)

    if (verboso) update_progress_bar(matched_rows, match_type)

    if (all(key_cols %in% names(input_padrao))) {

      # select match function
      match_fun <-
        if (match_type %in% c(number_exact_types, exact_types_no_number)) { match_cases
        } else if (match_type %in% number_interpolation_types ) { match_weighted_cases
        } else if (match_type %in% c(probabilistic_exact_types, probabilistic_types_no_number)) { match_cases_probabilistic
        } else if (match_type %in% probabilistic_interpolation_types) { match_weighted_cases_probabilistic
          }

      n_rows_affected <- match_fun(
        con,
        match_type = match_type,
        key_cols = key_cols,
        resultado_completo = resultado_completo
      )

      # n_rows_left <- DBI::dbGetQuery(con, 'SELECT COUNT(*) FROM input_padrao_db;')$`count_star()`
      # matched_rows <- n_rows - n_rows_left
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

  output_df <- merge_results(
    con,
    x='input_db',
    y='output_db',
    key_column='tempidgeocodebr',
    select_columns = x_columns,
    resultado_completo = resultado_completo
  )

  data.table::setDT(output_df)

  # Disconnect from DuckDB when done
  duckdb::dbDisconnect(con)

  # lida com casos de empate
  if (nrow(output_df) > n_rows) {

    # encontra casos de empate
    output_df[, empate := ifelse(.N > 1, TRUE, FALSE), by = tempidgeocodebr]

    # calcula distancias entre casos empatados
    output_df[empate == TRUE,
              dist_geocodebr := dt_haversine(
                lat, lon,
                data.table::shift(lat), data.table::shift(lon)
                ),
              by = tempidgeocodebr
              ]


    output_df[empate == TRUE,
              dist_geocodebr := ifelse(is.na(dist_geocodebr), 0, dist_geocodebr)
              ]

    # ignora casos com dist menor do q 200 metros
    output_df <- output_df[ empate==FALSE |
                            empate==TRUE & dist_geocodebr == 0 |
                            dist_geocodebr > 200
                            ]

    # update casos de empate
    output_df[, empate := ifelse(.N > 1, TRUE, FALSE), by = tempidgeocodebr]


    # conta numero de casos empatados
    ids_empate <- output_df[empate == TRUE, ]$tempidgeocodebr
    n_casos_empate <- unique(ids_empate) |> length()

   # drop geocodebr temp columns
   output_df[, dist_geocodebr := NULL]

   if (n_casos_empate >= 1 & isFALSE(resolver_empates)) {
     cli::cli_warn(
       "Foram encontrados {n_casos_empate} casos de empate. Estes casos foram marcados com valor igual `TRUE` na coluna 'empate',
       e podem ser inspecionados na coluna 'endereco_encontrado'. Alternativamente, use `resolver_empates==TRUE` para que o pacote
       lide com os empates automaticamente."
       )
     }

   if (n_casos_empate >= 1 & isTRUE(resolver_empates)) {

    # Keeping only unique rows based on all columns except 'score',
    # selecting the row with the max 'score'
    output_df <- output_df[output_df[, .I[contagem_cnefe == max(contagem_cnefe)], by = tempidgeocodebr]$V1]
    output_df <- output_df[output_df[, .I[1], by = tempidgeocodebr]$V1]
    output_df[, c('empate', 'contagem_cnefe') := NULL]

    cli::cli_warn(
       "Foram encontrados e resolvidos {n_casos_empate} casos de empate."
     )
   }
  }

  # drop geocodebr temp id column
  output_df[, tempidgeocodebr := NULL]


  # convert df to simple feature
  if (isTRUE(resultado_sf)) {
    output_sf <- sfheaders::sf_point(
      obj = output_df,
      x = 'lon',
      y = 'lat',
      keep = TRUE
      )

    sf::st_crs(output_sf) <- 4674
    return(output_sf)
  }

  return(output_df)
}
