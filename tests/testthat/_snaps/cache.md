# messages are formatted correctly

    Code
      set_cache_dir()
    Message <geocodebr_cache_dir>
      i Setting cache directory to '<path_to_default_dir>'.

---

    Code
      set_cache_dir("aaa")
    Message <geocodebr_cache_dir>
      i Setting cache directory to 'aaa'.

# behaves correctly

    Code
      res <- list_cached_data(print_tree = TRUE)
    Output
      <path_to_cache_dir>
      +-- estado=AC
      |   \-- part-0.parquet
      \-- estado=AL
          \-- part-0.parquet

