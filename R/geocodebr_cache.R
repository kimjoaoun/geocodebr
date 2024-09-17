#' Manage cached files from the geocodebr package
#'
#' @param list_files Logical. Whether to print a message with the address of all
#'        geocodebr data sets cached locally. Defaults to `TRUE`.
#' @param delete_file String. The file name (basename) of a geocodebr data set
#'        cached locally that should be deleted. Defaults to `NULL`, so that no
#'        file is deleted. If `delete_file = "all"`, then all cached geocodebr
#'        files are deleted.
#' @param silent Logical. Defaults to `TRUE` so messages are printed.

#' @return A message indicating which file exist and/or which ones have been
#'         deleted from local cache directory.
#' @export
#' @family Cache data
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' # list all files cached
#' geocodebr_cache(list_files = TRUE)
#'
#' # delete particular file
#' geocodebr_cache(delete_file = '2010_deaths')
#'
geocodebr_cache <- function(list_files = TRUE,
                            delete_file = NULL,
                            silent = FALSE
                            ){

  # check inputs
  checkmate::assert_logical(list_files)
  checkmate::assert_character(delete_file, null.ok = TRUE)

  # find / create local dir
  if (!dir.exists(geocodebr_env$cache_dir)) { dir.create(geocodebr_env$cache_dir, recursive=TRUE) }

  # list cached files
  files <- list.files(geocodebr_env$cache_dir, full.names = TRUE)
  files <- fs::path(files)

  # if wants to dele file
  # delete_file = "2_families.parquet"
  if (!is.null(delete_file)) {

    # IF file does not exist, print message
    if (!any(grepl(delete_file, files)) & delete_file != "all") {
      if (isTRUE(silent)) {
        suppressMessages({ message(paste0("The file '", delete_file, "' is not cached.")) })
      } else { message(paste0("The file '", delete_file, "' is not cached.")) }
    }

    # IF file exists, delete file
    if (any(grepl(delete_file, files))) {
      f <- files[grepl(delete_file, files)]
      unlink(f, recursive = TRUE)

      if (isTRUE(silent)) {
        suppressMessages({ message(paste0("The file '", delete_file, "' has been removed.")) })
      } else { message(paste0("The file '", delete_file, "' has been removed.")) }

    }

    # Delete ALL file
    if (delete_file=='all') {

      # delete any files from geocodebr, current and old data releases
      dir_above <- dirname(geocodebr_env$cache_dir)
      unlink(dir_above, recursive = TRUE)

      if (isTRUE(silent)) {
        suppressMessages({ message(paste0("All files have been removed.")) })
      } else { message(paste0("All files have been removed.")) }
    }
  }

  # list cached files
  files <- list.files(geocodebr_env$cache_dir, full.names = TRUE)
  files <- fs::path(files)

  # print file names
  if(isTRUE(list_files)){

    if (isTRUE(silent)) {
      suppressMessages({ message('Files currently chached:')
                         message(paste0(files, collapse = '\n')) })
    } else { message('Files currently chached:')
             message(paste0(files, collapse = '\n')) }
  }

  if (isFALSE(silent)) { return(files) }

}

