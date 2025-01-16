# set_cache_dir -----------------------------------------------------------

tester <- function(path = NULL) set_cache_dir(path)

test_that("errors with incorrect input", {
  expect_error(tester(1))
  expect_error(tester(c("aaa", "bbb")))
})

test_that("behaves correctly", {
  # if the cache config file exists, we save its current content just to make
  # sure our tests don't disturb any workflows we have. if it doesn't, we delete
  # the file we created during the test

  if (fs::file_exists(cache_config_file)) {
    config_file_content <- readLines(cache_config_file)
    on.exit(writeLines(config_file_content, cache_config_file), add = TRUE)
  } else {
    on.exit(fs::file_delete(cache_config_file), add = TRUE)
  }

  # by default uses a versioned dir inside the default R cache dir

  fn_result <- suppressMessages(set_cache_dir())
  expect_type(fn_result, "character")
  expect_identical(fn_result, as.character(default_cache_dir))
  expect_identical(readLines(cache_config_file), unclass(default_cache_dir))

  fn_result <- suppressMessages(set_cache_dir("aaa"))
  expect_type(fn_result, "character")
  expect_identical(fn_result, "aaa")
  expect_identical(readLines(cache_config_file), "aaa")
})

test_that("messages are formatted correctly", {
  if (fs::file_exists(cache_config_file)) {
    config_file_content <- readLines(cache_config_file)
    on.exit(writeLines(config_file_content, cache_config_file), add = TRUE)
  } else {
    on.exit(fs::file_delete(cache_config_file), add = TRUE)
  }

  expect_snapshot(
    set_cache_dir(),
    transform = function(x) sub(default_cache_dir, "<path_to_default_dir>", x),
    cnd_class = TRUE
  )

  expect_snapshot(set_cache_dir("aaa"), cnd_class = TRUE)
})

# get_cache_dir -----------------------------------------------------------

test_that("behaves correctly", {
  # if the cache config file exists, we save its current content just to make
  # sure our tests don't disturb any workflows we have. if it doesn't, we delete
  # the file we created during the test

  if (fs::file_exists(cache_config_file)) {
    config_file_content <- readLines(cache_config_file)
    on.exit(writeLines(config_file_content, cache_config_file), add = TRUE)
  } else {
    on.exit(fs::file_delete(cache_config_file), add = TRUE)
  }

  # if the cache config file exists, return its content. otherwise, returns the
  # default cache dir

  if (fs::file_exists(cache_config_file)) fs::file_delete(cache_config_file)
  expect_identical(get_cache_dir(), as.character(default_cache_dir))

  writeLines("aaa", cache_config_file)
  expect_identical(get_cache_dir(), "aaa")
})

# list_cached_data --------------------------------------------------------

test_that("errors with incorrect input", {
  expect_error(list_cached_data(1))
  expect_error(list_cached_data(NA))
  expect_error(list_cached_data(c(TRUE, TRUE)))
})

test_that("behaves correctly", {
  # if the cache config file exists, we save its current content just to make
  # sure our tests don't disturb any workflows we have. if it doesn't, we delete
  # the file we created during the test

  if (fs::file_exists(cache_config_file)) {
    config_file_content <- readLines(cache_config_file)
    on.exit(writeLines(config_file_content, cache_config_file), add = TRUE)
  } else {
    on.exit(fs::file_delete(cache_config_file), add = TRUE)
  }

  # we set the cache dir to a temporary directory to not mess with any cached
  # data we may already have

  tmpdir <- tempfile()
  fs::dir_create(tmpdir)

  suppressMessages(set_cache_dir(tmpdir))

  expect_identical(list_cached_data(), character(0))

  # previously, we used download_cnefe(progress = FALSE) here to download cnefe
  # data before listing the content. however, this takes a long time, and
  # afterall we only need to make sure that the function lists whatever files we
  # have in the directory. so we create empty temp files to test if the function
  # is working

  file.create(fs::path(tmpdir, c("oie.parquet", "hello.parquet")))

  cnefe_files <- list_cached_data()
  expect_identical(basename(cnefe_files), c("hello.parquet", "oie.parquet"))

  # expect a tree-like message and invisible value when print_tree=TRUE

  expect_snapshot(
    list_cached_data(print_tree = TRUE),
    transform = function(x) sub(get_cache_dir(), "<path_to_cache_dir>", x)
  )
})

# clean_cache_dir ---------------------------------------------------------

test_that("clean_cache_dir behaves correctly", {
  # if the cache config file exists, we save its current content just to make
  # sure our tests don't disturb any workflows we have. if it doesn't, we delete
  # the file we created during the test

  if (fs::file_exists(cache_config_file)) {
    config_file_content <- readLines(cache_config_file)
    on.exit(writeLines(config_file_content, cache_config_file), add = TRUE)
  } else {
    on.exit(fs::file_delete(cache_config_file), add = TRUE)
  }

  # we set the cache dir to a temporary directory to not mess with any cached
  # data we may already have

  tmpdir <- tempfile()
  fs::dir_create(tmpdir)

  suppressMessages(set_cache_dir(tmpdir))

  file.create(fs::path(tmpdir, "oie.parquet"))
  expect_identical(basename(list_cached_data()), "oie.parquet")

  expect_snapshot(
    res <- clean_cache_dir(),
    cnd_class = TRUE,
    transform = function(x) sub(get_cache_dir(), "<path_to_cache_dir>", x)
  )

  expect_identical(res, as.character(fs::path_norm(tmpdir)))
  expect_false(dir.exists(res))
})
