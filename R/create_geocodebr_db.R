#' Build duckdb connection with CNEFE data set
#'
#' Builds a duckdb connection with the CNEFE data set in the cache direcotry.
#'
#' @param db_path A character vector pointing to an existing directory where a
#'        a persistent '.duckdb' file is created. Defaults to a temporary
#'        directory `tempdir()`.
#' @param ncores Number of cores to be used in parallel execution. Defaults to
#'        the number of available cores minus 1.
#'
#' @return A duckdb connection.
#'
#' @family Support
#'
#' @keywords internal
#'
create_geocodebr_db <- function(db_path = tempdir(),
                                ncores = NULL
                                ){

  ## check input -------------------------------------------------------

  checkmate::assert_directory_exists(db_path)
  checkmate::assert_number(ncores, null.ok = TRUE)


  # check if CNEFE has been downloaded
  cache_dir <- geocodebr::get_cache_dir()
  cached_files <- list.files(cache_dir, recursive = TRUE, pattern = '.parquet')

  # if (length(cached_files)==0) {
  #   stop("No CNEFE data has been downloaded yet.")
  #   }

  ## create db connection -------------------------------------------------------

  ## this creates a persistent database which allows DuckDB to
  ## perform **larger-than-memory** workloads
  db_path <- fs::file_temp(tmp_dir = db_path, ext = '.duckdb')
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir=db_path)

  ## configure db connection  -------------------------------------------------------

  # Set Number of cores for parallel operation
  if (is.null(ncores)) {
    ncores <- parallel::detectCores()
    ncores <- ncores-1
    if (ncores<1) {ncores <- 1}
  }

  DBI::dbExecute(con, sprintf("SET threads = %s;", ncores))

  # Set Memory limit
  # TODO
  # DBI::dbExecute(con, "SET memory_limit = '20GB'")


  if (DBI::dbExistsTable(con, 'cnefe')){
    duckdb::duckdb_unregister_arrow(con, 'cnefe')
    gc()
  }

  # Load CNEFE data and write it to DuckDB
  cnefe <- arrow_open_dataset(cache_dir)
  duckdb::duckdb_register_arrow(con, "cnefe", cnefe)

  return(con)
  }
