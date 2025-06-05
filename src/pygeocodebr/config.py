"""
Configuration constants for PyGeocodeBR
"""

DATA_RELEASE = "v0.2.0"

ALL_FILES = [
    "municipio_logradouro_numero_localidade.parquet",
    "municipio_logradouro_numero_cep_localidade.parquet",
    "municipio.parquet",
    "municipio_cep.parquet",
    "municipio_cep_localidade.parquet",
    "municipio_localidade.parquet",
    "municipio_logradouro_cep_localidade.parquet",
    "municipio_logradouro_localidade.parquet",
]

ALL_POSSIBLE_MATCH_TYPES = [
    "dn01",
    "dn02",
    "dn03",
    "dn04",
    "pn01",
    "pn02",
    "pn03",
    "pn04",
    "da01",
    "da02",
    "da03",
    "da04",
    "pa01",
    "pa02",
    "pa03",
    "pa04",
    "dl01",
    "dl02",
    "dl03",
    "dl04",
    "pl01",
    "pl02",
    "pl03",
    "pl04",
    "dc01",
    "dc02",
    "db01",
    "dm01",
]

PRECISION_MAPPING = {
    "dn01": "numero",
    "dn02": "numero",
    "dn03": "numero",
    "dn04": "numero",
    "pn01": "numero",
    "pn02": "numero",
    "pn03": "numero",
    "pn04": "numero",
    "da01": "numero_aproximado",
    "da02": "numero_aproximado",
    "da03": "numero_aproximado",
    "da04": "numero_aproximado",
    "pa01": "numero_aproximado",
    "pa02": "numero_aproximado",
    "pa03": "numero_aproximado",
    "pa04": "numero_aproximado",
    "dl01": "logradouro",
    "dl02": "logradouro",
    "dl03": "logradouro",
    "dl04": "logradouro",
    "pl01": "logradouro",
    "pl02": "logradouro",
    "pl03": "logradouro",
    "pl04": "logradouro",
    "dc01": "cep",
    "dc02": "cep",
    "db01": "localidade",
    "dm01": "municipio",
}

GITHUB_RELEASE_URL = (
    f"https://github.com/ipeaGIT/padronizacao_cnefe/releases/download/{DATA_RELEASE}"
)

REQUIRED_INPUT_COLUMNS = ["id", "endereco", "municipio", "estado"]
OPTIONAL_INPUT_COLUMNS = ["logradouro", "numero", "cep", "localidade"]

DEFAULT_OUTPUT_COLUMNS = [
    "lat",
    "lon",
    "tipo_resultado",
    "precisao",
    "endereco_encontrado",
    "logradouro_encontrado",
    "contagem_cnefe",
]

COMPLETE_OUTPUT_COLUMNS = DEFAULT_OUTPUT_COLUMNS + [
    "numero_encontrado",
    "cep_encontrado",
    "localidade_encontrada",
    "municipio_encontrado",
    "estado_encontrado",
    "similaridade_logradouro",
]
