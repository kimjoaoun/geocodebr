data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
input_df <- read.csv(data_path)

fields <- setup_address_fields(
  logradouro = "nm_logradouro",
  numero = "Numero",
  cep = "Cep",
  bairro = "Bairro",
  municipio = "nm_municipio",
  estado = "nm_uf"
)

tester <- function(addresses_table = input_df,
                   address_fields = fields,
                   n_cores = 1,
                   progress = TRUE,
                   cache = TRUE) {
  geocode(addresses_table, address_fields, n_cores, progress, cache)
}

test_that("errors with incorrect input", {
  expect_error(tester(unclass(input_df)))

  expect_error(tester(address_fields = 1))
  expect_error(tester(address_fields = c(hehe = "nm_logradouro")))
  expect_error(tester(address_fields = c(logradouro = "hehe")))

  expect_error(tester(n_cores = "a"))
  expect_error(tester(n_cores = 0))
  expect_error(tester(n_cores = Inf))

  expect_error(tester(progress = 1))
  expect_error(tester(progress = NA))
  expect_error(tester(progress = c(TRUE, TRUE)))

  expect_error(tester(cache = 1))
  expect_error(tester(cache = NA))
  expect_error(tester(cache = c(TRUE, TRUE)))
})

