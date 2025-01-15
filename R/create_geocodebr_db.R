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



  # check input
  checkmate::assert_number(n_cores, null.ok = TRUE)


  # create db connection
  # this creates a local database which allows DuckDB to
  # perform **larger-than-memory** workloads
  db_path <- fs::file_temp(tmp_dir = db_path, ext = '.duckdb')
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir= db_path ) # db_path ":memory:"


  # Set Number of cores for parallel operation
  if (is.null(n_cores)) {
    n_cores <- parallel::detectCores()
    n_cores <- n_cores - 1
    if (n_cores<1) {n_cores <- 1}
  }

  DBI::dbExecute(con, sprintf("SET threads = %s;", n_cores))

  # test
  # Set Memory limit
  # DBI::dbExecute(con, "SET memory_limit = '8GB'")

  return(con)
  }
