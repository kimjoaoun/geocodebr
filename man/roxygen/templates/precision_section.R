#' @details Precision categories:
#'
#' # Precision
#'  The geocode results are classified into six broad `precision` categories:
#' - "numero"
#' - "numero_interpolado"
#' - "rua"
#' - "cep"
#' - "bairro"
#' - "municipio"
#' - `NA` (not found)
#'
#' Each precision level can be disaggregated into more refined match types.
#'
#' # Match Type
#' The column `match_type` provides more refined information on how exactly each
#' input address was matched with CNEFE. In every category, the function takes
#' the average latitude and longitude of the addresses included in CNEFE that
#' match the input address based on combinations of different fields. In the
#' strictest case, for example, the function finds a deterministic match for all
#' of the fields of a given address (estado, municipio, logradouro, numero, cep,
#' localidade). Think for example of a building with several apartments that
#' match the same street address and number. In such case, the coordinates of
#' the apartments will differ very slightly, and the function takes the average
#' of those coordinates. In a less rigorous example, in which only the fields
#' (estado, municipio, rua, bairro) are matched, the function calculates the
#' average coordinates of all the addresses in CNEFE along that street and which
#' fall within the same neighborhood.
#'
#' The complete list of precision levels and match type categories are:
#'
#' - Precision: **"numero"**
#'   - match_type:
#'     - en01: logradouro, numero, cep e bairro
#'     - en02: logradouro, numero e cep
#'     - en03: logradouro, numero e bairro
#'     - en04: logradouro e numero
#'     - pn01: logradouro, numero, cep e bairro
#'     - pn02: logradouro, numero e cep
#'     - pn03: logradouro, numero e bairro
#'     - pn04: logradouro e numero
#'
#' - Precision: **"numero_interpolado"**
#'   - match_type:
#'     - ei01: logradouro, numero, cep e bairro
#'     - ei02: logradouro, numero e cep
#'     - ei03: logradouro, numero e bairro
#'     - ei04: logradouro e numero
#'     - pi01: logradouro, numero, cep e bairro
#'     - pi02: logradouro, numero e cep
#'     - pi03: logradouro, numero e bairro
#'     - pi04: logradouro e numero
#'
#' - Precision: **"rua"** (when input number is missing 'S/N')
#'   - match_type:
#'     - er01: logradouro, cep e bairro
#'     - er02: logradouro e cep
#'     - er03: logradouro e bairro
#'     - er04: logradouro
#'     - pr01: logradouro, cep e bairro
#'     - pr02: logradouro e cep
#'     - pr03: logradouro e bairro
#'     - pr04: logradouro
#'
#' - Precision: **"cep"**
#'   - match_type:
#'     - ec01 municipio, cep, localidade
#'     - ec02 municipio, cep
#'
#' - Precision: **"bairro"**
#'   - match_type:
#'     - eb01 municipio, localidade
#'
#' - Precision: **"municipio"**
#'   - match_type:
#'     - em01 municipio
#'
#' Note: Match types starting with 'p' use probabilistic matching of the
#' logradouro field, while types starting with 'e' use deterministic matching
#' only. Match types with probabilistic matching ARE NOT implemented yet.
