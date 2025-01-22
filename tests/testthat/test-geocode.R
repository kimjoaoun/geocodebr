data_path <- system.file("extdata/small_sample.csv", package = "geocodebr")
input_df <- read.csv(data_path)

campos <- listar_campos(
  logradouro = "nm_logradouro",
  numero = "Numero",
  cep = "Cep",
  localidade = "Bairro",
  municipio = "nm_municipio",
  estado = "nm_uf"
)

tester <- function(enderecos = input_df,
                   campos_endereco = campos,
                   resultado_completo = FALSE,
                   verboso = TRUE,
                   cache = TRUE,
                   n_cores = 1) {
  geocode(enderecos, campos_endereco, resultado_completo, verboso, cache, n_cores)
}

test_that("expected output", {

  testthat::succeed( std_output <- tester() )

  # find expected match cases
  match_types_found <- unique(std_output$tipo_resultado)
  testthat::expect_true(length(match_types_found) == 16)

  # ful results
  testthat::succeed( full_output <- tester(resultado_completo = TRUE) )
  testthat::expect_true('endereco_encontrado' %in% names(full_output))

})





test_that("errors with incorrect input", {
  expect_error(tester(unclass(input_df)))

  expect_error(tester(campos_endereco = 1))
  expect_error(tester(campos_endereco = c(hehe = "nm_logradouro")))
  expect_error(tester(campos_endereco = c(logradouro = "hehe")))

  expect_error(tester(n_cores = "a"))
  expect_error(tester(n_cores = 0))
  expect_error(tester(n_cores = Inf))

  expect_error(tester(verboso = 1))
  expect_error(tester(verboso = NA))
  expect_error(tester(verboso = c(TRUE, TRUE)))

  expect_error(tester(cache = 1))
  expect_error(tester(cache = NA))
  expect_error(tester(cache = c(TRUE, TRUE)))

  expect_error(tester(resultado_completo = 1))
  expect_error(tester(resultado_completo = NA))
  expect_error(tester(resultado_completo = c(TRUE, TRUE)))

})

