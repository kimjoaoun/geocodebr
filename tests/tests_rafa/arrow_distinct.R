library(dplyr)
library(arrow)
library(geocodebr)

head(cad)

cad$id <- 1:nrow(cad)

fields_cad <- geocodebr::definir_campos(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  localidade = 'bairro',
  municipio = 'code_muni',
  estado = 'abbrev_state'
)

input_padrao <- enderecobr::padronizar_enderecos(
  cad,
  campos_do_endereco = enderecobr::correspondencia_campos(
    logradouro = fields_cad[["logradouro"]],
    numero = fields_cad[["numero"]],
    cep = fields_cad[["cep"]],
    bairro = fields_cad[["localidade"]],
    municipio = fields_cad[["municipio"]],
    estado = fields_cad[["estado"]]
  ),
  formato_estados = "sigla",
  formato_numeros = 'integer'
)

names(input_padrao)

arrow::write_parquet(input_padrao, 'input_padrao.parquet')

# unique_logradouros <- arrow::open_dataset('input_padrao.parquet') |>
#   dplyr::select(dplyr::all_of(key_cols)) |> # unique_cols
#   dplyr::distinct() |>
#   dplyr::compute()


input_padrao <- arrow::read_parquet('input_padrao.parquet')

input_states <- unique(input_padrao$estado_padr)
input_municipio <- unique(input_padrao$municipio_padr)



path_to_parquet <- geocodebr::listar_dados_cache()[7]

key_cols <- c("estado", "municipio", "logradouro", "numero", "cep" )


filtered_cnefe2 <- arrow::open_dataset(path_to_parquet) |>
  dplyr::filter(estado %in% input_states) |>
  dplyr::filter(municipio %in% input_municipio) |>
  dplyr::compute()


# unique_logradouros <- filtered_cnefe2 |>
unique_logradouros <- arrow::open_dataset(path_to_parquet) |>
  dplyr::select(dplyr::all_of(key_cols)) |>
  dplyr::distinct() |>
  dplyr::compute()
