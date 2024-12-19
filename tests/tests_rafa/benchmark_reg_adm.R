devtools::load_all('.')
library(tictoc)
library(dplyr)
library(data.table)
library(ipeadatalake)

# rais <- ipeadatalake::ler_rais(
#   ano = 2021,
#   tipo = 'estabelecimento',
#   as_data_frame = TRUE,
#   colunas = c("id_estab", "cnpj_raiz", "logradouro",
#               "bairro", "codemun", "uf", "cep", "qt_vinc_ativos")
#     )


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
  collect()


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
  addresses_table = cad2,
  address_fields = fields,
  n_cores = 12, # 7
  progress = T
)
tictoc::toc()

# 4.3 mi: 261.47 sec elapsed

a <- table(df_geo$match_type) / nrow(df_geo)*100
a
# case_01    case_02    case_04    case_05    case_06    case_07    case_08    case_09    case_10
# 2.5672246  1.7702695  0.8864906  1.1356360  0.4262566  0.1710496  0.1980082 50.2437217 39.1607998
# case_11
# 2.6746946
a <- as.data.table(a)
data.table::fwrite(a, 'a.csv', dec = ',', sep = '-')

