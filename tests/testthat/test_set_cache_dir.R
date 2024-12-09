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
  expect_s3_class(fn_result, class = c("fs_path", "character"), exact = TRUE)
  expect_identical(fn_result, default_cache_dir)
  expect_identical(readLines(cache_config_file), unclass(default_cache_dir))

  fn_result <- suppressMessages(set_cache_dir("aaa"))
  expect_s3_class(fn_result, class = c("fs_path", "character"), exact = TRUE)
  expect_identical(unclass(fn_result), "aaa")
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

