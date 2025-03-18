## R CMD check results

── R CMD check results ───────────────────────────────────────────────────────── geocodebr 0.1.19999 ────
Duration: 8m 15.8s

0 errors ✔ | 0 warnings ✔ | 0 notes ✔


## Major changes

- Function `geocode()` now includes probabilistic matching
- New function `buscapor_cep()`
- New function `geocode_reverso()`
- New argument in function `download_cnefe()`

## Bug fixes

- Solved bug related to columns with `integer64` in input tables

## Minor changes

- Ajuste na solução de casos de empate mais refinada e agora detalhada na documentação da função `geocode()`. [Encerra issue #37](https://github.com/ipeaGIT/geocodebr/issues/37). O método adotado na solução de empates agora fica transparente na documentação da função `geocode()`.
- Nova vignette sobre a função `geocode_reverso()`
- Vignette sobre *Get Started* e da função `geocode()` reorganizadas


