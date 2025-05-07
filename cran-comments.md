## R CMD check results

── R CMD check results ───────────────────────────────────────── geocodebr 0.2.0 ────
Duration: 6m 44.3s

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

- fixed error of corrupted data

## Novas funcionalidades (Major changes)

- A função `geocode()` agora inclui busca com match probabilistico. [Encerra issue #34](https://github.com/ipeaGIT/geocodebr/issues/34).
- Nova função `buscapor_cep()`. [Encerra issue #8](https://github.com/ipeaGIT/geocodebr/issues/8).
- Nova função `geocode_reverso()`. [Encerra issue #35](https://github.com/ipeaGIT/geocodebr/issues/35).
- A função `download_cnefe()` agora aceita o argumento `tabela` para baixar tabelas específicas.

## Correção de bugs ( Bug fixes)

- Resolvido bug que decaracterizava colunas de classe `integer64` na tabela de input de endereços. [Encerra issue #40](https://github.com/ipeaGIT/geocodebr/issues/40).


## Notas (Minor changes)

- Ajuste na solução de casos de empate mais refinada e agora detalhada na documentação da função `geocode()`. [Encerra issue #37](https://github.com/ipeaGIT/geocodebr/issues/37). O método adotado na solução de empates agora fica transparente na documentação da função `geocode()`.
- Nova vignette sobre a função `geocode_reverso()`
- Vignette sobre *Get Started* e da função `geocode()` reorganizadas


