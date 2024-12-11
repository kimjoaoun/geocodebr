#' Download the CNEFE data set
#'
#' Downloads an enriched version of the CNEFE (National Registry of Addresses
#' for Statistical Purposes, in portuguese) data set, purposefully built to be
#' used with this package.
#'
#' @param state A character vector. The states whose CNEFE data should be
#'   downloaded. Either `"all"` (the default), in which case the data for all
#'   states is downloaded, or a vector with the states abbreviations (e.g.
#'   `c("RJ", "DF")` to download the data for Rio de Janeiro and the Federal
#'   District).
#' @param progress A logical. Whether to display a download progress bar.
#'   Defaults to `TRUE`.
#' @template cache
#'
#' @return A directory path where the data was saved.
#'
#' @family Support
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' download_cnefe(state = "AC", progress = FALSE)
#'
#' download_cnefe(state = c("AC", "AL"), progress = FALSE)
#'
#' @export
download_cnefe <- function(state = "all", progress = TRUE, cache = TRUE) {
  checkmate::assert_logical(progress, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  state <- assert_and_assign_state(state)

  data_url <- glue::glue(
    "https://github.com/ipeaGIT/padronizacao_cnefe/releases/",
    "download/{data_release}/estado.{state}.zip"
  )

  if (!cache) {
    data_dir <- fs::path_norm(tempfile("standardized_cnefe"))
  } else {
    data_dir <- get_cache_dir()
  }
  fs::dir_create(data_dir)

  # we only need to download data that hasn't been downloaded yet. note that if
  # cache=FALSE data_dir is always empty, so we download all required data

  existing_data <- list.files(data_dir)
  existing_states <- substr(existing_data, start = 8, stop = 9)

  states_to_download <- setdiff(state, existing_states)
  files_to_download <- data_url[state %in% states_to_download]

  zip_paths <- download_files(files_to_download, progress)

  purrr::walk(
    zip_paths,
    function(zipfile) zip::unzip(zipfile, exdir = data_dir)
  )

  parquet_files <- list.files(data_dir, recursive = TRUE, full.names = TRUE)

  state_parquet_files <- parquet_files[
    grepl(glue::glue("estado=({paste(state, collapse = '|')})"), parquet_files)
  ]

  return(state_parquet_files)
}

assert_and_assign_state <- function(state) {
  all_states <- c(
    "RO", "AC", "AM", "RR", "PA", "AP", "TO", "MA", "PI", "CE", "RN", "PB",
    "PE", "AL", "SE", "BA", "MG", "ES", "RJ", "SP", "PR", "SC", "RS", "MS",
    "MT", "GO", "DF"
  )

  checkmate::assert_names(state, subset.of = c("all", all_states))

  if ("all" %in% state) state <- all_states

  return(state)
}

download_files <- function(files_to_download, progress) {
  # we always download the files to a temporary directory to prevent any
  # potential "garbage" in our cache dir (in case the download fails for some
  # reason or the unzipping process crashes mid-operation)

  download_dir <- tempfile("zipped_standardized_cnefe")
  fs::dir_create(download_dir)

  requests <- lapply(files_to_download, httr2::request)

  dest_files <- fs::path(download_dir, basename(files_to_download))

  responses <- perform_requests_in_parallel(requests, dest_files, progress)

  response_errored <- purrr::map_lgl(
    responses,
    function(r) inherits(r, "error")
  )

  # TODO: improve this error
  if (any(response_errored)) {
    stop("Could not download data for one of the states, uh-oh!")
  }

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

