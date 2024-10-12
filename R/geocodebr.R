#' geocodebr: Geocode Brazilian Addresses with CNEFE
#'
#' @section Usage:
#' Please check the vignettes and data documentation on the
#' [website](https://ipeagit.github.io/geocodebr/).
#'
#' @docType package
#' @name geocodebr
#' @aliases geocodebr-package
#'
#' @importFrom dplyr mutate select across case_when all_of
#' @importFrom data.table := .I .SD %chin% .GRP .N
#'
#' @keywords internal
"_PACKAGE"

## quiets concerns of R CMD check:
utils::globalVariables( c('year',
                          'temp_local_file') )

NULL
