#' @docType package
#' @name geocodebr
#' @aliases geocodebr-package
#'
#' @importFrom dplyr mutate select across case_when all_of
#' @importFrom data.table := .I .SD %chin% .GRP .N %like%
#'
#' @keywords internal
"_PACKAGE"

## quiets concerns of R CMD check:
utils::globalVariables(c('year',
                         'temp_local_file',
                         'lon_min',
                         'lon_max',
                         'lat_min',
                         'lat_min',
                         'lon_diff',
                         'lat_diff',
                         'lon',
                         'lat'))

data_release <- "v0.1.0"

default_cache_dir <- fs::path(
  tools::R_user_dir("geocodebr", which = "cache"),
  glue::glue("data_release_{data_release}")
)

cache_config_file <- fs::path(
  tools::R_user_dir("geocodebr", which = "config"),
  "cache_dir"
)
