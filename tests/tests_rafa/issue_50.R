library(readxl)
library(dplyr)
devtools::load_all('.')



enderecosbrasil <- readxl::read_xlsx('C:/Users/r1701707/Downloads/teste_cc_probabilistico_resultado.xlsx')

enderecosbrasil$CEP[1] <- NA

# Primeiro passo: inidicar o nome das colunas com cada campo dos enderecos
campos_1 <- geocodebr::definir_campos(
  logradouro = "Logradouro",
  numero = "Numero",
  cep = "CEP",
  localidade = "Bairro",
  municipio = "MUNICIPIO",
  estado = "UF"
)

enderecosbrasil <- dplyr::select(enderecosbrasil, Logradouro, Numero, CEP, Bairro, MUNICIPIO, UF)

# Segundo passo: geolocalizar
out_completo <- geocodebr::geocode(
  enderecos = enderecosbrasil,
  campos_endereco = campos_1,
  resultado_completo = TRUE,
  resolver_empates = F,
  resultado_sf = FALSE,
  verboso = T,
  cache = TRUE,
  n_cores = 1
)

View(out_completo)


# Segundo passo: geolocalizar
out_simles2 <- geocodebr::geocode(
  enderecos = enderecosbrasil,
  campos_endereco = campos_1,
  resultado_completo = FALSE,
  resolver_empates = F,
  resultado_sf = FALSE,
  verboso = T,
  cache = TRUE,
  n_cores = 1
)

head(out_simles2)

# logradouro_encontrado mesmo quando  resultado_completo = FALSE
