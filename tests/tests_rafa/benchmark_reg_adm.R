devtools::load_all('.')
library(tictoc)
library(dplyr)
library(data.table)
library(ipeadatalake)


# rais --------------------------------------------------------------------

rais <- ipeadatalake::ler_rais(
  ano = 2021,
  tipo = 'estabelecimento',
  as_data_frame = F,
  colunas = c("id_estab", "cnpj_raiz", "logradouro",
              "bairro", "codemun", "uf", "cep", "qt_vinc_ativos")
  ) |>
  filter(uf != "IG") |>
  filter(uf != "") |>
  collect()

# rais <- head(rais, n = 1000) |> collect() |> dput()
data.table::setDT(rais)

# create column number
rais[, numero := gsub("[^0-9]", "", logradouro)]

# remove numbers from logradouro
rais[, logradouro_no_numbers := gsub("\\d+", "", logradouro)]
rais[, logradouro_no_numbers := gsub(",", "", logradouro_no_numbers)]



# sample 10%
rais2 <- sample_frac(tbl = rais, 0.1)
gc(T)

tictoc::tic()
fields <- geocodebr::setup_address_fields(
  logradouro = 'logradouro_no_numbers',
  numero = 'numero',
  cep = 'cep',
  bairro = 'bairro',
  municipio = 'codemun',
  estado = 'uf'
)

df_geo <- geocodebr:::geocode(
  addresses_table = rais2,
  address_fields = fields,
  n_cores = 20, # 7
  progress = T
)
tictoc::toc()














# cad unico --------------------------------------------------------------------

cad <- ipeadatalake::ler_cadunico(
  data = 202312,
  tipo = 'familia',
  as_data_frame = F,
  colunas = c("co_familiar_fam", "co_uf", "cd_ibge_cadastro",
              "no_localidade_fam", "no_tip_logradouro_fam",
              "no_tit_logradouro_fam", "no_logradouro_fam",
              "nu_logradouro_fam", "ds_complemento_fam",
              "ds_complemento_adic_fam",
              "nu_cep_logradouro_fam", "co_unidade_territorial_fam",
              "no_unidade_territorial_fam", "co_local_domic_fam")
  )

# a <- tail(cad, n = 100) |> collect()

# compose address fields
cad <- cad |>
  mutate(no_tip_logradouro_fam = ifelse(is.na(no_tip_logradouro_fam), '', no_tip_logradouro_fam),
         no_tit_logradouro_fam = ifelse(is.na(no_tit_logradouro_fam), '', no_tit_logradouro_fam),
         no_logradouro_fam = ifelse(is.na(no_logradouro_fam), '', no_logradouro_fam)
         ) |>
  mutate(abbrev_state = co_uf,
          code_muni = cd_ibge_cadastro,
          logradouro = paste(no_tip_logradouro_fam, no_tit_logradouro_fam, no_logradouro_fam),
          numero = nu_logradouro_fam,
          cep = nu_cep_logradouro_fam,
          bairro = no_localidade_fam) |>
  select(co_familiar_fam,
         abbrev_state,
         code_muni,
         logradouro,
         numero,
         cep,
         bairro) |>
  dplyr::collect()


# sample 10%
# cad2 <- sample_frac(tbl = cad, 0.01)
gc(T)

tictoc::tic()
fields <- geocodebr::setup_address_fields(
  logradouro = 'logradouro',
  numero = 'numero',
  cep = 'cep',
  bairro = 'bairro',
  municipio = 'code_muni',
  estado = 'abbrev_state'
)

df_geo <- geocodebr:::geocode(
  addresses_table = cad,
  address_fields = fields,
  n_cores = 20, # 7
  progress = T
)
tictoc::toc()

# 43.8 mi: 1131.28 sec elapsed (19 min)

a <- table(df_geo$match_type) / nrow(df_geo)*100
a
a <- as.data.table(a)

data.table::fwrite(a, 'a.csv', dec = ',', sep = '-')

# numero
sum(a$N[which(a$V1 %like% '01|02|03|04|05')])
54.75767

# logradouro
sum(a$N[which(a$V1 %like% '01|02|03|04|05|06|07|08')])
62.745

