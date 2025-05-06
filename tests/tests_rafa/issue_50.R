library(readxl)
library(dplyr)
devtools::load_all('.')


path <- 'C:/Users/rafap/Downloads/teste_cc_enderecos.xlsx'
# 'C:/Users/r1701707/Downloads/teste_cc_probabilistico_resultado.xlsx'
enderecosbrasil <- readxl::read_xlsx(path)


enderecosbrasil$CEP[1] <- NA

enderecosbrasil <- enderecosbrasil[8,]

df <- structure(list(IBGE = 330455, UF = "RJ", MUNICIPIO = "Rio de Janeiro",
                     Endereco = "RUA DO RIO, 08  JACAREZINHO CEP: 20785-180",
                     Logradouro = "RUA DO RIO", Numero = 8, Bairro = "JACAREZINHO",
                     CEP = "20785-180", Observacao = "NA"), row.names = c(NA,
                                                                          -1L), class = c("tbl_df", "tbl", "data.frame"))


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

View(out_simles2)

# logradouro_encontrado mesmo quando  resultado_completo = FALSE
