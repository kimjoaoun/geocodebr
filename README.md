# dev

Reprex
```
library(geocodebr)

# open input data
data_path <- system.file("extdata/sample_1.csv", package = "geocodebr")
input_df <- read.csv(data_path)

geo_duck <- geocode(
  input_table = input_df,
  logradouro = "nm_logradouro",
  numero = "Numero",
  complemento = "Complemento",
  cep = "Cep",
  bairro = "Bairro",
  municipio = "nm_municipio",
  estado = "nm_uf",
  output_simple = TRUE,
  ncores = NULL,
  showProgress = TRUE
)

```
