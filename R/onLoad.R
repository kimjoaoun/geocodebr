# package global variables
geocodebr_env <- new.env(parent = emptyenv()) # nocov start

.onLoad <- function(libname, pkgname){

  # data release
  geocodebr_env$data_release <- 'v0.0.1'

  # local cache dir
  cache_d <- paste0('geocodebr/data_release_', geocodebr_env$data_release)
  geocodebr_env$cache_dir <- tools::R_user_dir(cache_d, which = 'cache')
  geocodebr_env$cache_dir <- fs::path(geocodebr_env$cache_dir)

  ## delete any files from old data releases
  dir_above <- fs::path_dir(geocodebr_env$cache_dir)
  all_cache <- list.files(dir_above, pattern = 'data_release',full.names = TRUE)
  old_cache <- all_cache[!grepl(geocodebr_env$data_release, all_cache)]
  if(length(old_cache)>0){ unlink(old_cache, recursive = TRUE) }

} # nocov end
