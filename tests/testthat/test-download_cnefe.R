tester <- function(progress = TRUE, cache = TRUE) {
  download_cnefe(progress, cache)
}

test_that("errors with incorrect input", {
  expect_error(tester(1))
  expect_error(tester("oie"))

  expect_error(tester(progress = 1))
  expect_error(tester(progress = NA))
  expect_error(tester(progress = c(TRUE, TRUE)))

  expect_error(tester(cache = 1))
  expect_error(tester(cache = NA))
  expect_error(tester(cache = c(TRUE, TRUE)))
})

test_that("returns the path to the directory where the files were saved", {
  result <- tester()
  expect_identical(result, file.path(get_cache_dir()))
})

test_that("cache usage is controlled by the cache argument", {
  result <- tester(cache = TRUE)
  expect_identical(result, file.path(get_cache_dir()))

  result <- tester(cache = FALSE)
  expect_true(
    grepl(file.path(fs::path_norm(tempdir()), "standardized_cnefe"), result)
  )
})

test_that("errors if could not download one or more files", {
  local_mocked_bindings(
    perform_requests_in_parallel = function(...) {
      httr2::req_perform_parallel(
        list(httr2::request("FAILURE")),
        on_error = "continue"
      )
    }
  )

  expect_error(
    tester("AL", cache = FALSE),
    class = "geocodebr_error_cnefe_download_failed"
  )

  expect_snapshot(tester("AL", cache = FALSE), error = TRUE, cnd_class = TRUE)
})

# test_that("would download the data of all states if state='all'", {
#   local_mocked_bindings(
#     perform_requests_in_parallel = function(requests, ...) {
#       if (length(requests) == 27) {
#         cli::cli_abort("Too much to download", class = "state_all_succeeded")
#       }
#     }
#   )
#
#   expect_error(tester("all", cache = FALSE), class = "state_all_succeeded")
# })
