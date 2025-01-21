#' Geolocaliza endereços no Brasil
#'
#' Geocodes Brazilian addresses based on CNEFE data. Addresses must be passed as
#' a data frame in which each column describes one address field (street name,
#' street number, neighborhood, etc). The input addresses are matched with CNEFE
#' following different precision levels For more info, please see the Details
#' section. The output coordinates use the geodetic reference system
#' "SIRGAS2000", CRS(4674).
#'
#' @param enderecos A data frame. The addresses to be geocoded. Each
#'   column must represent an address field.
#' @param campos_endereco A character vector. The correspondence between each
#'   address field and the name of the column that describes it in the
#'   `enderecos`. The [listar_campos()] function helps creating
#'   this vector and performs some checks on the input. Address fields
#'   passed as `NULL` are ignored and the function must receive at least one
#'   non-null field. If manually creating the vector, please note that the
#'   vector names should be the same names used in the [listar_campos()]
#'   parameters.
#' @param resultado_completo Logical. Whether the output should include additional
#'       columns, like the matched address of reference. Defaults to `FALSE`.
#' @template verboso
#' @template cache
#' @template n_cores
#'
#' @return Returns the data frame passed in `enderecos` with the latitude
#'   (`lat`) and longitude (`lon`) of each matched address, as well as two
#'   columns (`precision` and `match_type`) indicating the precision level with
#'   which the address was matched.
#'
#' @template precision_section
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#'
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
                    n_cores = 1
                    ){

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
    enderecos = enderecos,
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
  cols_padr <- grep("_padr", names(input_padrao), value = TRUE)
  input_padrao <- input_padrao[, .SD, .SDcols = cols_padr]
  names(input_padrao) <- gsub("_padr", "", cols_padr)

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
                       overwrite = TRUE, temporary = TRUE)

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

    key_cols <- get_relevant_cols_arrow(case)

    if (verboso) update_progress_bar(matched_rows, case)


    if (all(key_cols %in% names(input_padrao))) {

      # select match function
      match_fun <- ifelse(case %in% number_interpolation_types, match_weighted_cases, match_cases)

      n_rows_affected <- match_fun(
        con,
        x = 'input_padrao_db',
        y = 'filtered_cnefe', # keep this for now
        output_tb = paste0('output_', case),
        key_cols = key_cols,
        match_type = case,
        resultado_completo = resultado_completo
      )

      matched_rows <- matched_rows + n_rows_affected

      # leave the loop early if we find all addresses before covering all cases
      if (matched_rows == n_rows) break
    }

  }

  if (verboso) finish_progress_bar(matched_rows)


  # prepare output -----------------------------------------------
  # THIS could BE IMPROVED / optimized

  # list all table outputs
  all_possible_tables <- glue::glue("output_{all_possible_match_types}")

  # check which tables have been created
  output_tables <- lapply(
    X = all_possible_tables,
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
