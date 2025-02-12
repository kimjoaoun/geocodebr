# skip tests because they take too much time
skip_if(Sys.getenv("TEST_ONE") != "")
testthat::skip_on_cran()
testthat::skip_if_not_installed("arrow")


tester <- function(verboso = TRUE, cache = TRUE) {
  download_cnefe(verboso, cache)
}

test_that("errors with incorrect input", {
  expect_error(tester(1))
  expect_error(tester(NA))
  expect_error(tester(c(TRUE, TRUE)))

  expect_error(tester(cache = 1))
  expect_error(tester(cache = NA))
  expect_error(tester(cache = c(TRUE, TRUE)))
})

test_that("returns the path to the directory where the files were saved", {
  result <- tester()
  expect_identical(result, file.path(listar_pasta_cache()))
})

test_that("cache usage is controlled by the cache argument", {
  result <- tester(cache = TRUE)
  expect_identical(result, file.path(listar_pasta_cache()))

  # using a mocked binding for perform_requests_in_parallel here just to save us
  # some time. as long as none of its elements is a failed request, the funtion
  # will make download_files return the path to the files that would be
  # downloaded, which is basically what we want to test here

  local_mocked_bindings(
    perform_requests_in_parallel = function(...) TRUE
  )

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
    tester(cache = FALSE),
    class = "geocodebr_error_cnefe_download_failed"
  )

  expect_snapshot(tester(cache = FALSE), error = TRUE, cnd_class = TRUE)
})
