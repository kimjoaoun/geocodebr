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
#' df <- geocodebr::geocode(
#'   input_table = input_df,
#'   logradouro = "nm_logradouro",
#'   numero = "Numero",
#'   complemento = "Complemento",
#'   cep = "Cep",
#'   bairro = "Bairro",
#'   municipio = "code_muni",
#'   estado = "abbrev_state"
#'   )
#'

geocode <- function(input_table,
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
    logradouro,
    numero,
    complemento,
    cep,
    bairro,
    municipio,
    estado
  )

  # padroniza input do usuario
  input_padrao <- enderecopadrao::padronizar_enderecos(
    enderecos = input_table,
    campos_do_endereco = campos
    )

  input_padrao <- cbind(input_table, input_padrao) # REMOVER quando EP manter ID

  # convert standartdized input to arrow
  input_arrw_1 <- arrow::as_arrow_table(input_padrao)

  # determine states present in the input
  input_states <- unique(input_padrao$estado)
  input_states <- c("AC", "AL", "RJ") ############ REMOVER

  ### 1 download cnefe no cache
  download_success <- download_cnefe(input_states)

  # check if download worked
  if (isFALSE(download_success)) { return(invisible(NULL)) }

  # open cnefe parquet
  cnefe <- arrow_open_dataset(geocodebr_env$cache_dir)

  # narrow scope of search
  input_states <- unique(input_padrao$estado)
  input_ceps <- unique(input_padrao$cep)
  cnefe <- dplyr::filter(cnefe, estado %in% input_states)
  cnefe <- dplyr::filter(cnefe, cep %in% input_ceps) |>
    dplyr::compute()

  ### START DETERMINISTIC MATCHING

  #   - case 1: match municipio, logradouro, cep, bairro
  #   - case 2: match municipio, logradouro, cep
  #   - case 3: match municipio, logradouro, bairro
  #   - case 4: match municipio, logradouro

  # case 1
  cols_1 <- c("estado", "municipio", "logradouro", "numero", "cep", "bairro")
  cols_1g <- c('ID', cols_1)
  output_caso_1 <- dplyr::left_join(
    input_arrw_1,
    cnefe,
    by = cols_1,
  ) |> group_by_at(cols_1g) |>
  summarise(lon = mean(lon, na.rm=TRUE),
            lat = mean(lat, na.rm=TRUE)) |>
    filter(!is.na(lon)) |>
    compute()

  # drop NAs
  todo_ids <- setdiff(input_arrw_1$ID, output_caso_1$ID)
  input_arrw_2 <- dplyr::filter(input_arrw_1, ID %in% todo_ids) |>
    compute()

  # case 2
  cols_2 <- c("estado", "municipio", "logradouro", "numero", "cep")
  cols_2g <- c('ID', cols_2)
  output_caso_2 <- dplyr::left_join(
    input_arrw_2,
    cnefe,
    by = cols_2,
  ) |> group_by_at(cols_2g) |>
    summarise(lon = mean(lon, na.rm=TRUE),
              lat = mean(lat, na.rm=TRUE)) |>
    filter(!is.na(lon)) |>
    compute()

  # drop NAs
  todo_ids <- setdiff(input_arrw_2$ID, output_caso_2$ID)
  input_arrw_3 <- dplyr::filter(input_arrw_1, ID %in% todo_ids) |>
    compute()

  # case 3
  cols_3 <- c("estado", "municipio", "logradouro", "numero", "bairro")
  cols_3g <- c('ID', cols_3)
  output_caso_3 <- dplyr::left_join(
    input_arrw_3,
    cnefe,
    by = cols_3,
  ) |> group_by_at(cols_3g) |>
    summarise(lon = mean(lon, na.rm=TRUE),
              lat = mean(lat, na.rm=TRUE)) |>
    filter(!is.na(lon)) |>
    compute()


  # drop NAs
  todo_ids <- setdiff(input_arrw_3$ID, output_caso_3$ID)
  input_arrw_4 <- dplyr::filter(input_arrw_1, ID %in% todo_ids) |>
    compute()

  # case 4
  cols_4 <- c("estado", "municipio", "logradouro", "numero")
  cols_4g <- c('ID', cols_4)
  output_caso_4 <- dplyr::left_join(
    input_arrw_4,
    cnefe,
    by = cols_4,
  ) |> group_by_at(cols_4g) |>
    summarise(lon = mean(lon, na.rm=TRUE),
              lat = mean(lat, na.rm=TRUE)) |>
    filter(!is.na(lon)) |>
    compute()


  # add accuracy column
  output_caso_1 <- dplyr::mutate(output_caso_1, accuracy_g = 1L) |> compute()
  output_caso_2 <- dplyr::mutate(output_caso_2, accuracy_g = 2L) |> compute()
  output_caso_3 <- dplyr::mutate(output_caso_3, accuracy_g = 3L) |> compute()
  output_caso_4 <- dplyr::mutate(output_caso_4, accuracy_g = 4L) |> compute()


  # rbind deterministic results
  output_deterministic <- lapply(
    X= c(output_caso_1, output_caso_2, output_caso_3, output_caso_4),
    FUN = dplyr::collect) |>
    rbindlist(fill = TRUE
              )
  return(output_deterministic)

  #   # futuro
  #   - join probabilistico
  #   - interpolar numeros na mesma rua

}

