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
      list_cached_data(print_tree = TRUE)
    Output
      <path_to_cache_dir>
      +-- hello.parquet
      \-- oie.parquet

