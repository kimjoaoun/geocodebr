tester <- function(logradouro = NULL,
                   numero = NULL,
                   complemento = NULL,
                   cep = NULL,
                   bairro = NULL,
                   municipio = NULL,
                   estado = NULL) {
  setup_address_fields(
    logradouro,
    numero,
    complemento,
    cep,
    bairro,
    municipio,
    estado
  )
}

test_that("errors with incorrect input", {
  expect_error(tester(logradouro = 1))
  expect_error(tester(logradouro = c("aaa", "bbb")))

  expect_error(tester(numero = 1))
  expect_error(tester(numero = c("aaa", "bbb")))

  expect_error(tester(complemento = 1))
  expect_error(tester(complemento = c("aaa", "bbb")))

  expect_error(tester(cep = 1))
  expect_error(tester(cep = c("aaa", "bbb")))

  expect_error(tester(bairro = 1))
  expect_error(tester(bairro = c("aaa", "bbb")))

  expect_error(tester(municipio = 1))
  expect_error(tester(municipio = c("aaa", "bbb")))

  expect_error(tester(estado = 1))
  expect_error(tester(estado = c("aaa", "bbb")))
})

test_that("errors when all fields are NULL", {
  expect_error(tester(), class = "geocodebr_error_null_address_fields")

  expect_snapshot(tester(), error = TRUE, cnd_class = TRUE)
})

test_that("returns a character vector", {
  expect_identical(
    tester(
      logradouro = "Nome_logradouro",
      numero = "Numero",
      complemento = "Comp",
      cep = "CEP",
      bairro = "Bairro",
      municipio = "Cidade",
      estado = "UF"
    ),
    c(
      logradouro = "Nome_logradouro",
      numero = "Numero",
      complemento = "Comp",
      cep = "CEP",
      bairro = "Bairro",
      municipio = "Cidade",
      estado = "UF"
    )
  )

  expect_identical(
    tester(logradouro = "oi", numero = "ola"),
    c(logradouro = "oi", numero = "ola")
  )
})
