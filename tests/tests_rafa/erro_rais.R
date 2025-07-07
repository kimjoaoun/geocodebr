library(arrow)
#library(geocodebr)
devtools::load_all('.')
fff <- 'C:/Users/rafap/Downloads/rais_error.parquet'

df <- arrow::read_parquet(fff)

df <- df |> dplyr::select('id_estab', 'cei_vinc', 'logradouro', 'uf', 'codemun', 'cep' , 'bairro')

campos_do_endereco <- geocodebr::definir_campos(
  logradouro = "logradouro",
  estado = "uf",
  municipio = "codemun",
  cep = "cep",
  localidade = "bairro"
)

geo <- geocodebr::geocode(
  df,
  campos_endereco = campos_do_endereco,
  resolver_empates = T,
  resultado_sf = T,
  n_cores = 4,
  verboso = T
)


# ℹ Padronizando endereços de entrada
# ℹ Utilizando dados do CNEFE armazenados localmente
# ℹ Geolocalizando endereços
# Error in `dbSendQuery()`:15 ■                                  0% - Procurando pl01
# ! rapi_prepare: Failed to prepare query CREATE OR REPLACE VIEW unique_logradouros AS
# SELECT DISTINCT estado, municipio, logradouro, cep, localidade
# FROM unique_logradouros_cep_localidade;
# Error: Catalog Error: Table with name unique_logradouros_cep_localidade does not exist!
#   Did you mean "pg_description"?
#
#   LINE 3: FROM unique_logradouros_cep_localidade;
# ^
#   Run `rlang::last_trace()` to see where the error occurred.
# Endereços processados: 0/15 ■                                  0% - Procurando pl01




geo2 <- geocodebr::geocode(
  df,
  campos_endereco = geocodebr::definir_campos("estado", "municipio", "logradouro", "numero",
                                              "cep", "bairro"),
  resolver_empates = T,
  resultado_sf = T,
)
