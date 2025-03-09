# skip tests because they take too much time
skip_if(Sys.getenv("TEST_ONE") != "")
testthat::skip_on_cran()
testthat::skip_if_not_installed("arrow")


# amostra de pontos espaciais
points <- readRDS(
  system.file("extdata/pontos.rds", package = "geocodebr")
)

points <- points[1:20,]



tester <- function(pontos = points,
                   dist_max = 1000,
                   verboso = FALSE,
                   cache = TRUE,
                   n_cores = 1) {
  geocode_reverso(
    pontos = pontos,
    dist_max = dist_max,
    verboso = verboso,
    cache = cache,
    n_cores =n_cores
  )
}

test_that("expected output", {

  # radio de busca de 1 Km
  testthat::expect_success(std_output <- tester())
  testthat::expect_true(nrow(std_output) == 3)

  # radio de busca de 5 Km
  testthat::expect_success(std_output_5K <- tester(dist_max = 5000))
  testthat::expect_true(nrow(std_output_5K) == 15)

  # output in sf format
  testthat::expect_true(is(std_output , 'sf'))
})





test_that("errors with incorrect input", {

  # input nao eh sf
  expect_error(tester(unclass(points)))

  # input tem geometry diferente de POINT
  expect_error( tester(pontos = sf::st_cast(points, "LINESTRING")) )

  # input na projecao espacial errada
  expect_error(tester(sf::st_transform(points, 4326)))

  # input fora do bbox do Brazil
  p <- sf::st_sfc(sf::st_point(c(53.12682, 25.61657)))
  p <- sf::st_sf(p)
  sf::st_crs(p) <- 4674
  expect_error(tester(p))



  expect_error(tester(dist_max = 'A'))
  expect_error(tester(dist_max = NA))
  expect_error(tester(dist_max = TRUE))

  expect_error(tester(n_cores = "a"))
  expect_error(tester(n_cores = 0))
  expect_error(tester(n_cores = Inf))

  expect_error(tester(verboso = 1))
  expect_error(tester(verboso = NA))
  expect_error(tester(verboso = c(TRUE, TRUE)))

  expect_error(tester(cache = 1))
  expect_error(tester(cache = NA))
  expect_error(tester(cache = c(TRUE, TRUE)))
})

