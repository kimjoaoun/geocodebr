data_release <- "v0.1.0"

default_cache_dir <- fs::path(
  tools::R_user_dir("geocodebr", which = "cache"),
  glue::glue("data_release_{data_release}")
)

cache_config_file <- fs::path(
  tools::R_user_dir("geocodebr", which = "config"),
  "cache_dir"
)

# TODO: remove this environment after transition to new structure is done
# package global variables
geocodebr_env <- new.env(parent = emptyenv())

.onLoad <- function(libname, pkgname){
  fs::dir_create(default_cache_dir)

  # TODO: remove calls related to custom environment after transition to new
  # structure is done

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

}
