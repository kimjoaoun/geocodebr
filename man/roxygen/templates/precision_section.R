#' @details Precisão dos resultados:
#'
#' Os resultados do **{geocodebr}** são classificados em seis amplas categorias de `precisao`:
#'
#' - "numero"
#' - "numero_interpolado"
#' - "rua"
#' - "cep"
#' - "localidade"
#' - "municipio"
#' - `NA` (não encontrado)
#'
#' Cada nível de precisão pode ser desagregado em tipos de correspondência mais
#' refinados.
#'
#' # Tipos de resultados
#' A coluna `match_type` fornece informações mais detalhadas sobre como
#' exatamente cada endereço de entrada foi encontrado no CNEFE. Em cada
#' categoria,  o **{geocodebr}** calcula a média da latitude e longitude dos
#' endereços incluídos no CNEFE que correspondem ao endereço de entrada, com
#' base em combinações de diferentes campos. No caso mais rigoroso, por exemplo,
#' a função encontra uma correspondência determinística para todos os campos de
#' um dado endereço (`"estado"`, `"municipio"`, `"logradouro"`, `"numero"`,
#' `"cep"`, `"localidade"`). Pense, por exemplo, em um prédio com vários
#' apartamentos que correspondem ao mesmo endereço de rua e número. Nesse caso,
#' as coordenadas dos apartamentos podem diferir ligeiramente, e o
#' **{geocodebr}** calcula a média dessas coordenadas. Em um caso menos rigoroso,
#' no qual apenas os campos (`"estado"`, `"municipio"`, `"logradouro"`,
#' `"localidade"`) são encontrados, o **{geocodebr}** calcula as coordenadas
#' médias de todos os endereços no CNEFE ao longo daquela rua e que se encontram
#' na mesma localidade/bairro. Assim, as coordenadas de resultado tendem a ser o
#' ponto médio do trecho daquela rua que passa dentro daquela localidade/bairro.
#'
#' A lista completa dos níveis de precisão (`precisao`), suas categorias de tipo
#' de correspondência (`tipo_resultado`) e os campos de endereço considerados em
#' cada categoria estão descritos abaixo:
#'
#' - precisao: **"numero"**
#'   - tipo_resultado:
#'    - en01: logradouro, numero, cep e localidade
#'    - en02: logradouro, numero e cep
#'    - en03: logradouro, numero e localidade
#'    - en04: logradouro e numero
#'    - pn01: logradouro, numero, cep e localidade
#'    - pn02: logradouro, numero e cep
#'    - pn03: logradouro, numero e localidade
#'    - pn04: logradouro e numero
#'
#' - precisao: **"numero_aproximado"**
#'   - tipo_resultado:
#'    - ei01: logradouro, numero, cep e localidade
#'    - ei02: logradouro, numero e cep
#'    - ei03: logradouro, numero e localidade
#'    - ei04: logradouro e numero
#'    - pi01: logradouro, numero, cep e localidade
#'    - pi02: logradouro, numero e cep
#'    - pi03: logradouro, numero e localidade
#'    - pi04: logradouro e numero
#'
#' - precisao: **"logradouro"** (quando o número de entrada está faltando 'S/N')
#'   - tipo_resultado:
#'      - er01: logradouro, cep e localidade
#'      - er02: logradouro e cep
#'      - er03: logradouro e localidade
#'      - er04: logradouro
#'      - pr01: logradouro, cep e localidade
#'      - pr02: logradouro e cep
#'      - pr03: logradouro e localidade
#'      - pr04: logradouro
#'
#' - precisao: **"cep"**
#'   - tipo_resultado:
#'      - ec01: municipio, cep, localidade
#'      - ec02: municipio, cep
#'
#' - precisao: **"localidade"**
#'   - tipo_resultado:
#'      - eb01: municipio, localidade
#'
#' - precisao: **"municipio"**
#'   - tipo_resultado:
#'      - em01: municipio
#'
#' ***Nota:*** As categorias de `match_type` que começam com 'p' utilizam
#' correspondência probabilística do campo logradouro, enquanto os tipos que
#' começam com 'e' utilizam apenas correspondência determinística. **As
#' categorias de `tipo_resultado` que usam correspondência probabilística ainda
#' não estão implementadas no {geocodebr}**.
