tester <- function(state = "all", progress = TRUE, cache = TRUE) {
  download_cnefe(state, progress, cache)
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

test_that("returns the path to the downloaded files", {
  result <- tester("AL")
  expect_identical(
    result,
    file.path(get_cache_dir(), "estado=AL/part-0.parquet")
  )

  states <- c("AC", "AL")
  result <- tester(states)
  expect_identical(
    result,
    file.path(get_cache_dir(), glue::glue("estado={states}/part-0.parquet"))
  )
})

test_that("cache usage is controlled by the cache argument", {
  result <- tester("AL", cache = TRUE)
  expect_identical(
    result,
    file.path(get_cache_dir(), "estado=AL/part-0.parquet")
  )

  result <- tester("AL", cache = FALSE)
  expect_true(
    grepl(fs::path(fs::path_norm(tempdir()), "standardized_cnefe"), result)
  )
})

test_that("errors if could not download the data for one or more states", {
  local_mocked_bindings(
    perform_requests_in_parallel = function(...) {
      httr2::req_perform_parallel(
        list(httr2::request("FAILURE")),
        on_error = "continue"
      )
    }
  )

  expect_error(tester("AL", cache = FALSE))
})

test_that("would download the data of all states if state='all'", {
  local_mocked_bindings(
    perform_requests_in_parallel = function(requests, ...) {
      if (length(requests) == 27) {
        cli::cli_abort("Too much to download", class = "state_all_succeeded")
      }
    }
  )

  expect_error(tester("all", cache = FALSE), class = "state_all_succeeded")
})
