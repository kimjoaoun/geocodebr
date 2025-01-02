
# precision ------------------------------------------------------------------

input_df <- input_table <- data.frame(
  id=666,
  nm_logradouro = 'SQS 308 Bloco C',
  Numero = 204,
  Complemento = 'Bloco C',
  Cep = 70355030,
  Bairro = 'Asa sul',
  nm_municipio = 'Brasilia',
  nm_uf = 'DF'
)

cnefe_cep <- arrow::open_dataset( geocodebr::get_cache_dir())
df <- filter(cnefe_cep, cep =='70355-030') |> collect()



################## calculate precision as the area in m2
range_lon <- max(df$lon) - min(df$lon)
range_lat <- max(df$lat) - min(df$lat)

lon_meters <- 111320 * range_lon * cos(mean(df$lat))
lat_meters <- 111320 * range_lat

area = pi * lon_meters * lat_meters
area
##################
range_lon <- sd(df$lon) *2
range_lat <- sd(df$lat) *2

