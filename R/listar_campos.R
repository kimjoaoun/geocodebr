#' Specify the columns describing the address fields
#'
#' Creates a character vector specifying the columns that represent each address
#' field in the addresses table. Os campos `estado` e `municipio` são obrigatórios.
#'
#' @param estado A string. The name of the column representing the state of the
#'   address.
#' @param municipio A string. The name of the column representing the city of
#'   the address.
#' @param logradouro A string. The name of the column representing the
#'   *logradouro* (street address) of the address. May be `NULL` if the field is
#'   not specified in the addresses table.
#' @param numero A string. The name of the column representing the street number
#'   of the address. May be `NULL` if the field is not specified in the
#'   addresses table.
#' @param cep A string. The name of the column representing the *CEP* (ZIP code)
#'   of the address. May be `NULL` if the field is not specified in the
#'   addresses table.
#' @param localidade Uma string. O nome da localidade (equivalente a 'bairro' em
#'   areas urbanas) do endereço. Pode ser `NULL` se esse campo não estiver
#'   presente na tabela de endereços.
#'
#' @return A character vector in which the names are the address fields and the
#'   values are the columns that represent them in the addresses table.
#'
#' @examples
#' listar_campos(
#'   logradouro = "Nome_logradouro",
#'   numero = "Numero",
#'   cep = "CEP",
#'   localidade = "Bairro",
#'   municipio = "Cidade",
#'   estado = "UF"
#' )
#'
#' @export
listar_campos <- function(estado,
                          municipio,
                          logradouro = NULL,
                          numero = NULL,
                          cep = NULL,
                          localidade = NULL) {

  col <- checkmate::makeAssertCollection()
  checkmate::assert_string(logradouro, null.ok = TRUE, add = col)
  checkmate::assert_string(numero, null.ok = TRUE, add = col)
  checkmate::assert_string(cep, null.ok = TRUE, add = col)
  checkmate::assert_string(localidade, null.ok = TRUE, add = col)
  checkmate::assert_string(municipio, null.ok = TRUE, add = col)
  checkmate::assert_string(estado, null.ok = TRUE, add = col)
  checkmate::reportAssertions(col)

  address_fields <- c(
    logradouro = logradouro,
    numero = numero,
    cep = cep,
    localidade = localidade,
    municipio = municipio,
    estado = estado
  )

  if (is.null(address_fields)) error_null_address_fields()

  return(address_fields)
}

error_null_address_fields <- function() {
  geocodebr_error(
    "Pelo menos um campo não pode ser nulo {.code NULL}.",
    call = rlang::caller_env()
  )
}
