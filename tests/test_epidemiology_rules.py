import json
from pathlib import Path
import unittest

import epidemiology_rules as epi


def temporal_points(values, counts=None):
    counts = counts or [3 for _ in values]
    return [
        {"period": f"2025-{index:02d}", "value": value, "count": counts[index - 1]}
        for index, value in enumerate(values, start=1)
    ]


def fixture_trend(values, counts):
    return [
        {"period": f"2025-{index:02d}", "value": value, "count": counts[index - 1]}
        for index, value in enumerate(values, start=1)
    ]


class EpidemiologyRulesTests(unittest.TestCase):
    def test_sns_domain_fixtures_calibrate_zero_reporting_and_density(self):
        fixtures_path = Path(__file__).parent / "fixtures" / "epidemiology_domain_fixtures.json"
        fixtures = json.loads(fixtures_path.read_text(encoding="utf-8"))

        for fixture in fixtures:
            with self.subTest(fixture=fixture["name"]):
                review = epi.build_epidemiology_review(
                    dataset_id=fixture["dataset_id"],
                    dataset_title=fixture["dataset_title"],
                    mega_theme=fixture["mega_theme"],
                    fields=fixture["fields"],
                    numeric_profiles=fixture["numeric_profiles"],
                    categorical_profiles=fixture["categorical_profiles"],
                    trends=[{"points": fixture_trend(fixture["values"], fixture["counts"])}],
                    temporal_field=fixture["temporal_field"],
                    ordering=fixture["ordering"],
                    quality_summary=fixture["quality_summary"],
                    quality_warnings=[],
                )

                expected = fixture["expected"]
                self.assertEqual(review["surveillance_domain"]["code"], expected["domain"])
                self.assertEqual(review["time_axis"]["status"], expected["time_axis_status"])
                self.assertEqual(review["zero_meaning"]["status"], expected["zero_status"])
                self.assertEqual(review["reporting_process"]["status"], expected["reporting_status"])
                self.assertTrue(review["rule_trace"], "fixture must leave auditable rule trace")

    def test_population_coverage_zeros_are_fragile(self):
        review = epi.build_epidemiology_review(
            dataset_id="cobertura-utentes",
            dataset_title="Taxa de cobertura da população inscrita",
            mega_theme="Acesso & Produção",
            fields=[{"name": "periodo"}, {"name": "taxa_cobertura"}, {"name": "utentes_inscritos"}],
            numeric_profiles=[{"field": "taxa_cobertura", "label": "% Cobertura", "measure_role": "taxa"}],
            categorical_profiles=[{"field": "regiao", "count": 6, "missing": 0, "unique": 2}],
            trends=[{"points": temporal_points([0, 12, 0, 14, 15, 16])}],
            temporal_field="periodo",
            ordering="temporal_desc",
            quality_summary={"coverage": {"status": "pronto", "ratio": 1}, "denominator": {"status": "rever", "label": "validar unidade"}, "granularity": {"status": "pronto", "label": "granularidade legível"}},
            quality_warnings=[],
        )

        self.assertEqual(review["surveillance_domain"]["code"], "cobertura_populacional")
        self.assertEqual(review["zero_meaning"]["status"], "fragil")
        self.assertTrue(any(item["rule_id"] == "ZERO-001" for item in review["rule_trace"]))

    def test_rare_event_zeros_are_not_fragile_by_default(self):
        review = epi.build_epidemiology_review(
            dataset_id="mortalidade-eventos",
            dataset_title="Óbitos por evento raro",
            mega_theme="Saúde Pública & Emergência",
            fields=[{"name": "periodo"}, {"name": "obitos"}],
            numeric_profiles=[{"field": "obitos", "label": "Óbitos", "measure_role": "contagem"}],
            categorical_profiles=[],
            trends=[{"points": temporal_points([0, 1, 0, 0, 1, 0])}],
            temporal_field="periodo",
            ordering="api_default",
            quality_summary={"coverage": {"status": "pronto", "ratio": 1}, "denominator": {"status": "pronto", "label": "medida simples"}, "granularity": {"status": "pronto", "label": "granularidade legível"}},
            quality_warnings=[],
        )

        self.assertEqual(review["surveillance_domain"]["code"], "eventos_raros")
        self.assertIn(review["zero_meaning"]["status"], {"pronto", "rever"})
        self.assertNotEqual(review["zero_meaning"]["status"], "fragil")

    def test_activity_zero_tail_is_fragile(self):
        review = epi.build_epidemiology_review(
            dataset_id="consultas-atividade",
            dataset_title="Consultas médicas por mês",
            mega_theme="Acesso & Produção",
            fields=[{"name": "periodo"}, {"name": "consultas"}],
            numeric_profiles=[{"field": "consultas", "label": "Consultas", "measure_role": "contagem"}],
            categorical_profiles=[],
            trends=[{"points": temporal_points([100, 120, 140, 0, 0, 0])}],
            temporal_field="periodo",
            ordering="temporal_desc",
            quality_summary={"coverage": {"status": "rever", "ratio": 0.5}, "denominator": {"status": "pronto", "label": "medida simples"}, "granularity": {"status": "pronto", "label": "granularidade legível"}},
            quality_warnings=[],
        )

        self.assertEqual(review["surveillance_domain"]["code"], "atividade_assistencial")
        self.assertEqual(review["zero_meaning"]["status"], "fragil")
        self.assertTrue(any(item["rule_id"] == "ZERO-003" for item in review["rule_trace"]))

    def test_financial_zeros_are_review_not_ready(self):
        review = epi.build_epidemiology_review(
            dataset_id="despesa-sns",
            dataset_title="Despesa do Serviço Nacional de Saúde",
            mega_theme="Finanças & Compras",
            fields=[{"name": "periodo"}, {"name": "valor"}],
            numeric_profiles=[{"field": "valor", "label": "Valor", "measure_role": "monetario"}],
            categorical_profiles=[],
            trends=[{"points": temporal_points([1000, 0, 1200, 1300, 0, 1400])}],
            temporal_field="periodo",
            ordering="api_default",
            quality_summary={"coverage": {"status": "pronto", "ratio": 1}, "denominator": {"status": "rever", "label": "validar unidade"}, "granularity": {"status": "pronto", "label": "granularidade legível"}},
            quality_warnings=[],
        )

        self.assertEqual(review["surveillance_domain"]["code"], "financeiro")
        self.assertEqual(review["zero_meaning"]["status"], "rever")
        self.assertTrue(any(item["rule_id"] == "ZERO-004" for item in review["rule_trace"]))

    def test_short_time_axis_and_density_drop_block(self):
        short = epi.build_epidemiology_review(
            dataset_id="atividade-curta",
            dataset_title="Produção assistencial",
            mega_theme="Acesso & Produção",
            fields=[{"name": "periodo"}, {"name": "producao"}],
            numeric_profiles=[{"field": "producao", "label": "Produção", "measure_role": "contagem"}],
            categorical_profiles=[],
            trends=[{"points": temporal_points([10, 12])}],
            temporal_field="periodo",
            ordering="api_default",
            quality_summary={"coverage": {"status": "pronto", "ratio": 1}, "denominator": {"status": "pronto", "label": "medida simples"}, "granularity": {"status": "pronto", "label": "granularidade legível"}},
            quality_warnings=[],
        )
        lag = epi.build_epidemiology_review(
            dataset_id="atividade-lag",
            dataset_title="Produção assistencial",
            mega_theme="Acesso & Produção",
            fields=[{"name": "periodo"}, {"name": "producao"}],
            numeric_profiles=[{"field": "producao", "label": "Produção", "measure_role": "contagem"}],
            categorical_profiles=[],
            trends=[{"points": temporal_points([10, 12, 14, 16, 18, 20, 1, 1], counts=[10, 10, 10, 10, 10, 10, 1, 1])}],
            temporal_field="periodo",
            ordering="temporal_desc",
            quality_summary={"coverage": {"status": "rever", "ratio": 0.4}, "denominator": {"status": "pronto", "label": "medida simples"}, "granularity": {"status": "pronto", "label": "granularidade legível"}},
            quality_warnings=[],
        )

        self.assertTrue(any(item["rule_id"] == "TIME-001" for item in short["blocking_factors"]))
        self.assertTrue(any(item["rule_id"] == "REPORT-002" for item in lag["blocking_factors"]))

    def test_unit_cost_invalid_denominator_adds_denom_blocker(self):
        review = epi.build_finprod_epidemiology_review(
            denominator_valid=False,
            numerator_valid=True,
            shared_periods=["2025-01", "2025-02", "2025-03"],
            rows=[{"period": "2025-01", "production_value": 2}],
            pairs=[],
            benchmark_rows=[],
            blockers=[],
            unit_warnings=[],
            shared_granularity="mensal",
        )

        self.assertTrue(any(item["rule_id"] == "DENOM-002" for item in review["blocking_factors"]))
        self.assertEqual(review["denominator"]["status"], "fragil")


if __name__ == "__main__":
    unittest.main()
