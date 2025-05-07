# geocodebr 0.2.0

## Mudanças grandes (Major changes)

- A função `geocode()` agora inclui busca com match probabilistico. [Encerra issue #34](https://github.com/ipeaGIT/geocodebr/issues/34).
- Nova função `buscapor_cep()`. [Encerra issue #8](https://github.com/ipeaGIT/geocodebr/issues/8).
- Nova função `geocode_reverso()`. [Encerra issue #35](https://github.com/ipeaGIT/geocodebr/issues/35).
- A função `download_cnefe()` agora aceita o argumento `tabela` para baixar tabelas específicas.

## Mudanças pequenas (Minor changes)

- Ajuste na solução de casos de empate mais refinada e agora detalhada na documentação da função `geocode()`. [Encerra issue #37](https://github.com/ipeaGIT/geocodebr/issues/37). O método adotado na solução de empates agora fica transparente na documentação da função `geocode()`.
- Nova vignette sobre a função `geocode_reverso()`
- Vignette sobre *Get Started* e da função `geocode()` reorganizadas

## Correção de bugs (Bug fixes)

- Resolvido bug que decaracterizava colunas de classe `integer64` na tabela de input de endereços. [Encerra issue #40](https://github.com/ipeaGIT/geocodebr/issues/40).

## Novos contribuidores (New contributions)

- Arthur Bazzolli

# geocodebr 0.1.1

## Correção de bugs

- Corrigido bug na organização de pastas do cache de dados. Fecha o [issue 29](https://github.com/ipeaGIT/geocodebr/issues/29).


# geocodebr 0.1.0

- Primeira versão estável.
