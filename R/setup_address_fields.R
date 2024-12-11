#' Specify the columns describing the address fields
#'
#' Creates a character vector specifying the columns that represent each address
#' field in the addresses table.
#'
#' @param logradouro A string. The name of the column representing the
#'   *logradouro* (street address) of the address. May be `NULL` if the field is
#'   not specified in the addresses table.
#' @param numero A string. The name of the column representing the street number
#'   of the address. May be `NULL` if the field is not specified in the
#'   addresses table.
#' @param complemento A string. The name of the column representing the
#'   *complemento* (additional information such as apartment, PO box, etc) of
#'   the address. May be `NULL` if the field is not specified in the addresses
#'   table.
#' @param cep A string. The name of the column representing the *CEP* (ZIP code)
#'   of the address. May be `NULL` if the field is not specified in the
#'   addresses table.
#' @param bairro A string. The name of the column representing the neighborhood
#'   of the address. May be `NULL` if the field is not specified in the
#'   addresses table.
#' @param municipio A string. The name of the column representing the city of
#'   the address. May be `NULL` if the field is not specified in the addresses
#'   table.
#' @param estado A string. The name of the column representing the state of the
#'   address. May be `NULL` if the field is not specified in the addresses
#'   table.
#'
#' @return A character vector in which the names are the address fields and the
#'   values are the columns that represent them in the addresses table.
#'
#' @examples
#' setup_address_fields(
#'   logradouro = "Nome_logradouro",
#'   numero = "Numero",
#'   complemento = "Comp",
#'   cep = "CEP",
#'   bairro = "Bairro",
#'   municipio = "Cidade",
#'   estado = "UF"
#' )
#'
#' @export
setup_address_fields <- function(logradouro = NULL,
                                 numero = NULL,
                                 complemento = NULL,
                                 cep = NULL,
                                 bairro = NULL,
                                 municipio = NULL,
                                 estado = NULL) {
  col <- checkmate::makeAssertCollection()
  checkmate::assert_string(logradouro, null.ok = TRUE, add = col)
  checkmate::assert_string(numero, null.ok = TRUE, add = col)
  checkmate::assert_string(complemento, null.ok = TRUE, add = col)
  checkmate::assert_string(cep, null.ok = TRUE, add = col)
  checkmate::assert_string(bairro, null.ok = TRUE, add = col)
  checkmate::assert_string(municipio, null.ok = TRUE, add = col)
  checkmate::assert_string(estado, null.ok = TRUE, add = col)
  checkmate::reportAssertions(col)

  address_fields <- c(
    logradouro = logradouro,
    numero = numero,
    complemento = complemento,
    cep = cep,
    bairro = bairro,
    municipio = municipio,
    estado = estado
  )

  if (is.null(address_fields)) error_null_address_fields()

  return(address_fields)
}

error_null_address_fields <- function() {
  geocodebr_error(
    paste0(
      "At least one of {.fn address_fields_const} ",
      "arguments must not be {.code NULL}."
    ),
    call = rlang::caller_env()
  )
}
