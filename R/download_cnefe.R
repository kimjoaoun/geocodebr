#' Download the CNEFE data set
#'
#' Downloads an enriched version of the CNEFE (National Registry of Addresses
#' for Statistical Purposes, in portuguese) data set, purposefully built to be
#' used with this package.
#'
#' @param progress A logical. Whether to display a download progress bar.
#'   Defaults to `TRUE`.
#' @template cache
#'
#' @return Invisibly returns the path to the directory where the data was saved.
#'
#' @family Support
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#'
#' download_cnefe(progress = FALSE)
#'
#'
#' @export
download_cnefe <- function(progress = TRUE, cache = TRUE) {

  checkmate::assert_logical(progress, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)

  all_files <- c(
  "municipio_logradouro_numero_localidade.parquet",  # 4 largest files
  "municipio.parquet",
  "municipio_cep.parquet",
  "municipio_cep_localidade.parquet",
  "municipio_logradouro_numero_cep_localidade.parquet", # 4 largest files
  "municipio_localidade.parquet",
  "municipio_logradouro.parquet",
  "municipio_logradouro_numero_cep.parquet", # 4 largest files
  "municipio_logradouro_cep.parquet",
  "municipio_logradouro_cep_localidade.parquet",
  "municipio_logradouro_numero.parquet", # 4 largest files
  "municipio_logradouro_localidade.parquet"
  )

  data_urls <- glue::glue(
    "https://github.com/ipeaGIT/padronizacao_cnefe/releases/",
    "download/{data_release}/{all_files}"
  )

  if (!cache) {
    data_dir <- as.character(fs::path_norm(tempfile("standardized_cnefe")))
  } else {
    data_dir <- get_cache_dir()
  }
  fs::dir_create(data_dir)

  # we only need to download data that hasn't been downloaded yet. note that if
  # cache=FALSE data_dir is always empty, so we download all required data

  existing_files <- list.files(data_dir)

  files_to_download <- setdiff(all_files, existing_files)
  files_to_download <- data_urls[all_files %in% files_to_download]

  downloaded_files <- download_files(files_to_download, progress)

  return(invisible(data_dir))
}


download_files <- function(files_to_download, progress) {
  # we always download the files to a temporary directory to prevent any
  # potential "garbage" in our cache dir (in case the download fails for some
  # reason or the unzipping process crashes mid-operation)

  download_dir <- geocodebr::get_cache_dir()

  requests <- lapply(files_to_download, httr2::request)

  dest_files <- fs::path(download_dir, basename(files_to_download))

  responses <- perform_requests_in_parallel(requests, dest_files, progress)

  response_errored <- purrr::map_lgl(
    responses,
    function(r) inherits(r, "error")
  )

  if (any(response_errored)) error_cnefe_download_failed()

  return(dest_files)
}

perform_requests_in_parallel <- function(requests, dest_files, progress) {
  # we create this wrapper around httr2::req_perform_parallel just for testing
  # purposes. it's easier to mock this function when testing than to mock a
  # function from another package.
  #
  # related test: "errors if could not download the data for one or more states"
  # in test-download_cnefe
  #
  # related help page:
  # https://testthat.r-lib.org/reference/local_mocked_bindings.html

  httr2::req_perform_parallel(
    requests,
    paths = dest_files,
    on_error = "continue",
    progress = ifelse(progress == TRUE, "Downloading CNEFE data", FALSE)
  )
}

error_cnefe_download_failed <- function() {
  geocodebr_error(
    c(
      "Could not download CNEFE data for one or more files",
      "i" = "Please try again later."
    ),
    call = rlang::caller_env(n = 2)
  )
}
