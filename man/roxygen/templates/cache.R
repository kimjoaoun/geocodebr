#' @param cache Logical. Whether the function should read the data cached
#'        locally, which is much faster. Defaults to `TRUE`. The first time the
#'        user runs the function, `geocodebr` will download CNEFE data and store
#'        it locally so that the data only needs to be download once. If `FALSE`,
#'        the function will download the data again and overwrite the local files.
