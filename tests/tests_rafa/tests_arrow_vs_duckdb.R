library(geocodebr)
library(tictoc)
library(enderecopadrao)
library(data.table)
library(dplyr)
library(arrow)
library(duckdb)


# open input data
data_path <- system.file("extdata/sample_1.csv", package = "geocodebr")
input_df <- read.csv(data_path)

input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)
input_df <- rbind(input_df,input_df,input_df,input_df,input_df,input_df,input_df,input_df)

# setDT(input_df)
input_df$ID <-  1:nrow(input_df)

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




tictoc::tic()
df_duck2 <- geocode_duck2(
  input_table = input_df,
  logradouro = "nm_logradouro",
  numero = "Numero",
  complemento = "Complemento",
  cep = "Cep",
  bairro = "Bairro",
  municipio = "nm_municipio",
  estado = "nm_uf"
)
tictoc::toc()
#> 18: 3.0
#> 4mi: 40.2 sec elapsed
