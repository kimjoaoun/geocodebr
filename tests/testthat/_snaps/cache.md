# messages are formatted correctly

    Code
      definir_pasta_cache()
    Message <geocodebr_cache_dir>
      i Setting cache directory to '<path_to_default_dir>'.

---

    Code
      definir_pasta_cache("aaa")
    Message <geocodebr_cache_dir>
      i Setting cache directory to 'aaa'.

# behaves correctly

    Code
      listar_dados_cache(print_tree = TRUE)
    Output
      <path_to_cache_dir>
      +-- hello.parquet
      \-- oie.parquet

# deletar_pasta_cache behaves correctly

    Code
      res <- deletar_pasta_cache()
    Message <geocodebr_message_removed_cache_dir>
      v Deleted cache directory previously located at '<path_to_cache_dir>'.

