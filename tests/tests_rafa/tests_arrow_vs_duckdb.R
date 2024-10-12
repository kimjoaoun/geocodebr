library(geocodebr)
library(tictoc)
library(enderecopadrao)
library(data.table)
library(dplyr)
library(arrow)


# open input data
data_path <- system.file("extdata/sample_1.csv", package = "geocodebr")
input_df <- read.csv(data_path)


tic()
df_arrow <- geocodebr::geocode(
 input_table = input_df,
 logradouro = "nm_logradouro",
 numero = "Numero",
 complemento = "Complemento",
 cep = "Cep",
 bairro = "Bairro",
 municipio = "nm_municipio",
 estado = "nm_uf"
 )

toc()
#> 17.6  | 14.7
#>
#>
#>
#>


tic()
df_duck <- geocode_duck(
  input_table = input_df,
  logradouro = "nm_logradouro",
  numero = "Numero",
  complemento = "Complemento",
  cep = "Cep",
  bairro = "Bairro",
  municipio = "nm_municipio",
  estado = "nm_uf"
)

toc()
#> 3.5


