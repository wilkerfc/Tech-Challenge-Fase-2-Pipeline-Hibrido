"""
Testes Unitários – Silver Layer
"""

import pandas as pd
import pytest

import sys
sys.path.insert(0, "/home/claude/tc-fase2")

from pipeline.silver.transform import (
    cast_types,
    fill_missing,
    normalize_text_columns,
    remove_duplicates,
    validate_referential_integrity,
    transform_ufs,
    transform_municipios,
    transform_indicador,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_ufs() -> pd.DataFrame:
    return pd.DataFrame({
        "id_uf": ["35", "33", "31"],
        "sigla": ["SP", "RJ", "MG"],
        "nome": ["São Paulo", "Rio de Janeiro", "Minas Gerais"],
        "_ingestion_timestamp": ["2024-01-01", "2024-01-01", "2024-01-01"],
        "_source": ["basedosdados"] * 3,
        "_batch_date": ["2024-01-01"] * 3,
    })


@pytest.fixture
def sample_municipios(sample_ufs) -> pd.DataFrame:
    return pd.DataFrame({
        "id_municipio": ["3550308", "3304557", "3106200"],
        "nome": ["São Paulo", "Rio de Janeiro", "Belo Horizonte"],
        "sigla_uf": ["SP", "RJ", "MG"],
        "id_uf": ["35", "33", "31"],
        "_ingestion_timestamp": ["2024-01-01"] * 3,
        "_source": ["basedosdados"] * 3,
        "_batch_date": ["2024-01-01"] * 3,
    })


@pytest.fixture
def sample_indicador() -> pd.DataFrame:
    return pd.DataFrame({
        "id_municipio": ["3550308", "3304557", "3106200", "3550308"],
        "ano": [2022, 2022, 2022, 2023],
        "indicador_alfabetizacao": [72.5, 68.1, 75.0, 74.3],
        "quantidade_matriculas": [10000, 8000, 7000, 10200],
        "_ingestion_timestamp": ["2024-01-01"] * 4,
        "_source": ["basedosdados"] * 4,
        "_batch_date": ["2024-01-01"] * 4,
    })


# ---------------------------------------------------------------------------
# Testes genéricos
# ---------------------------------------------------------------------------

class TestRemoveDuplicates:
    def test_remove_exact_duplicates(self):
        df = pd.DataFrame({"id": [1, 1, 2], "val": ["a", "a", "b"]})
        result = remove_duplicates(df, subset=["id"])
        assert len(result) == 2

    def test_no_duplicates_unchanged(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = remove_duplicates(df, subset=["id"])
        assert len(result) == 3


class TestFillMissing:
    def test_fills_nulls(self):
        df = pd.DataFrame({"nome": [None, "SP", None]})
        result = fill_missing(df, {"nome": "NAO INFORMADO"})
        assert result["nome"].isna().sum() == 0
        assert result["nome"].iloc[0] == "NAO INFORMADO"

    def test_ignores_missing_column(self):
        df = pd.DataFrame({"col_a": [1, 2]})
        # Não deve lançar exceção
        result = fill_missing(df, {"col_inexistente": 0})
        assert "col_inexistente" not in result.columns


class TestNormalizeText:
    def test_removes_accents_and_upper(self):
        df = pd.DataFrame({"nome": ["são paulo", "minas gerais"]})
        result = normalize_text_columns(df, ["nome"])
        assert result["nome"].iloc[0] == "SAO PAULO"
        assert result["nome"].iloc[1] == "MINAS GERAIS"

    def test_strips_whitespace(self):
        df = pd.DataFrame({"nome": ["  SP  "]})
        result = normalize_text_columns(df, ["nome"])
        assert result["nome"].iloc[0] == "SP"


class TestCastTypes:
    def test_casts_int_column(self):
        df = pd.DataFrame({"ano": ["2022", "2023"]})
        result = cast_types(df, {"ano": int})
        assert result["ano"].dtype == int

    def test_tolerates_failed_cast(self):
        df = pd.DataFrame({"val": ["abc", "def"]})
        # Não deve lançar exceção
        result = cast_types(df, {"val": float})
        assert "val" in result.columns


class TestReferentialIntegrity:
    def test_removes_invalid_keys(self, sample_ufs):
        df_with_invalid = pd.DataFrame({
            "id_municipio": ["VALID", "INVALID"],
            "sigla_uf": ["SP", "XX"],  # XX não existe em UFs
        })
        result = validate_referential_integrity(
            df_with_invalid, sample_ufs, "sigla_uf", "sigla", "test"
        )
        assert len(result) == 1
        assert result["sigla_uf"].iloc[0] == "SP"


# ---------------------------------------------------------------------------
# Testes de transformações por entidade
# ---------------------------------------------------------------------------

class TestTransformUFs:
    def test_returns_expected_columns(self, sample_ufs):
        result = transform_ufs(sample_ufs)
        expected_cols = {"id_uf", "sigla", "nome", "_silver_timestamp"}
        assert expected_cols.issubset(set(result.columns))

    def test_no_duplicates(self, sample_ufs):
        # Adiciona duplicata
        dup = pd.concat([sample_ufs, sample_ufs.iloc[[0]]], ignore_index=True)
        result = transform_ufs(dup)
        assert result["id_uf"].duplicated().sum() == 0

    def test_text_normalized(self, sample_ufs):
        result = transform_ufs(sample_ufs)
        assert result["sigla"].str.isupper().all()


class TestTransformMunicipios:
    def test_removes_invalid_uf(self, sample_municipios, sample_ufs):
        # Adiciona município com UF inválida
        invalid = pd.DataFrame({
            "id_municipio": ["9999999"],
            "nome": ["Cidade Fantasma"],
            "sigla_uf": ["ZZ"],
            "id_uf": ["99"],
            "_ingestion_timestamp": ["2024-01-01"],
            "_source": ["test"],
            "_batch_date": ["2024-01-01"],
        })
        df = pd.concat([sample_municipios, invalid], ignore_index=True)
        ufs_silver = transform_ufs(sample_ufs)
        result = transform_municipios(df, ufs_silver)
        assert "9999999" not in result["id_municipio"].values

    def test_valid_municipios_kept(self, sample_municipios, sample_ufs):
        ufs_silver = transform_ufs(sample_ufs)
        result = transform_municipios(sample_municipios, ufs_silver)
        assert len(result) == 3


class TestTransformIndicador:
    def test_meta_atingida_flag(self, sample_indicador, sample_municipios, sample_ufs):
        ufs_silver = transform_ufs(sample_ufs)
        mun_silver = transform_municipios(sample_municipios, ufs_silver)
        result = transform_indicador(sample_indicador, mun_silver)
        # Todos indicadores acima de 50 devem ter meta_atingida = True
        above_50 = result[result["indicador_alfabetizacao"] > 50]
        assert above_50["meta_atingida"].all()

    def test_removes_duplicates_by_municipio_ano(self, sample_indicador, sample_municipios, sample_ufs):
        # Adiciona duplicata
        dup = pd.concat([sample_indicador, sample_indicador.iloc[[0]]], ignore_index=True)
        ufs_silver = transform_ufs(sample_ufs)
        mun_silver = transform_municipios(sample_municipios, ufs_silver)
        result = transform_indicador(dup, mun_silver)
        assert not result.duplicated(subset=["id_municipio", "ano"]).any()


# ---------------------------------------------------------------------------
# Testes de qualidade (smoke tests)
# ---------------------------------------------------------------------------

class TestDataQualityChecks:
    def test_indicador_range_valid(self, sample_indicador):
        from scripts.data_quality import check_indicador_range
        result = check_indicador_range(sample_indicador, "indicador_alfabetizacao", "silver")
        assert result.passed

    def test_indicador_range_invalid(self):
        from scripts.data_quality import check_indicador_range
        df = pd.DataFrame({"indicador_alfabetizacao": [50.0, 150.0, -5.0]})
        result = check_indicador_range(df, "test", "silver")
        assert not result.passed
        assert result.failed_count == 2

    def test_valid_years(self, sample_indicador):
        from scripts.data_quality import check_valid_years
        result = check_valid_years(sample_indicador, "indicador", "silver")
        assert result.passed

    def test_invalid_years(self):
        from scripts.data_quality import check_valid_years
        df = pd.DataFrame({"ano": [2022, 1999, 2031]})
        result = check_valid_years(df, "test", "silver")
        assert not result.passed
        assert result.failed_count == 2
