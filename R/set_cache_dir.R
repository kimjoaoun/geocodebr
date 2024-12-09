#' Set the cache directory used in geocodebr
#'
#' Sets the directory used to cache CNEFE data. This configuration is persistent
#' across different R sessions.
#'
#' @param path A string. The path to the directory used to cache the data. If
#'   `NULL` (the default), the package will use a versioned directory saved
#'   inside the directory returned by [tools::R_user_dir].
#'
#' @return Invisibly returns the cache directory path.
#'
#' @examples
#' set_cache_dir(tempdir())
#'
#' # back to default
#' set_cache_dir(NULL)
#'
#' @export
set_cache_dir <- function(path = NULL) {
  checkmate::assert_string(path, null.ok = TRUE)

  if (is.null(path)) {
    cache_dir <- default_cache_dir
  } else {
    cache_dir <- fs::path_norm(path)
  }

  cli::cli_inform(
    c(
      "i" = "Setting cache directory to {.file {cache_dir}}.",
      class = "geocodebr_cache_dir"
    )
  )

  if (!fs::file_exists(cache_config_file)) {
    fs::dir_create(fs::path_dir(cache_config_file))
    fs::file_create(cache_config_file)
  }

  writeLines(cache_dir, con = cache_config_file)

  return(invisible(cache_dir))
}
