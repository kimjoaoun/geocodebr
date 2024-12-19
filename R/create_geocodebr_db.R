#' Build duckdb connection with CNEFE data set
#'
#' Builds a duckdb connection with the CNEFE data set in the cache direcotry.
#'
#' @param db_path A character vector pointing to an existing directory where a
#'        a persistent '.duckdb' file is created. Defaults to a temporary
#'        directory `tempdir()`.
#' @template n_cores
#'
#' @return A duckdb connection.
#'
#' @family Support
#'
#' @keywords internal
#'
create_geocodebr_db <- function(db_path = tempdir(),
                                n_cores = NULL
                                ){



  ## check input -------------------------------------------------------

  checkmate::assert_number(n_cores, null.ok = TRUE)


  ## create db connection -------------------------------------------------------

  # # remove traces of previous db
  # old_duckdb <- list.files( db_path, pattern = '.duckdb', full.names = TRUE)
  #
  # if (length(old_duckdb)>0) {
  #   unlink(old_duckdb, recursive = TRUE, force = TRUE, expand = TRUE)
  #   }


  ## this creates a persistent database which allows DuckDB to
  ## perform **larger-than-memory** workloads
  db_path <- fs::file_temp(tmp_dir = db_path, ext = '.duckdb')
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir= db_path ) # db_path ":memory:"

  ## configure db connection  -------------------------------------------------------

  # Set Number of cores for parallel operation
  if (is.null(n_cores)) {
    n_cores <- parallel::detectCores()
    n_cores <- n_cores-1
    if (n_cores<1) {n_cores <- 1}
  }

  DBI::dbExecute(con, sprintf("SET threads = %s;", n_cores))

  # Set Memory limit
  # TODO
  # DBI::dbExecute(con, "SET memory_limit = '20GB'")


  if (DBI::dbExistsTable(con, 'cnefe')){
    duckdb::duckdb_unregister_arrow(con, 'cnefe')
    gc()
  }

  return(con)
  }
