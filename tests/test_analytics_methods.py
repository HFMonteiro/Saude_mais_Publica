import unittest
from unittest.mock import patch

import server


def fake_dataset(records_count=12):
    return {
        "fields": [
            {"name": "periodo", "label": "Periodo", "type": "date"},
            {"name": "valor", "label": "Valor", "type": "integer"},
            {"name": "producao", "label": "Producao", "type": "integer"},
            {"name": "regiao", "label": "Regiao", "type": "text"},
        ],
        "metas": {"default": {"title": "Dataset sintético", "records_count": records_count}},
    }


def fake_records():
    rows = []
    for month in range(1, 13):
        rows.append(
            {
                "periodo": f"2025-{month:02d}",
                "valor": month * 10,
                "producao": month * 2,
                "regiao": "Norte" if month <= 6 else "Sul",
            }
        )
    return {"results": rows, "total_count": len(rows)}


def geo_dataset(records_count=5):
    return {
        "fields": [
            {"name": "localizacao", "label": "Localização Geográfica", "type": "geo point"},
            {"name": "regiao", "label": "Região", "type": "text"},
            {"name": "valor", "label": "Valor", "type": "integer"},
        ],
        "metas": {"default": {"title": "Dataset geográfico", "records_count": records_count}},
    }


def geo_records():
    return {
        "results": [
            {"localizacao": {"lon": -7.27322, "lat": 40.50314}, "regiao": "Centro", "valor": 1},
            {"localizacao": {"lon": -7.486884, "lat": 39.831269}, "regiao": "Centro", "valor": 2},
            {"localizacao": {"lon": -6.759102, "lat": 41.806816}, "regiao": "Norte", "valor": 3},
            {"localizacao": {"lon": -7.42635033349663, "lat": 39.55}, "regiao": "Centro", "valor": 4},
            {"localizacao": {"lon": -7.501801, "lat": 40.279552}, "regiao": "Centro", "valor": 5},
        ],
        "total_count": 5,
    }


def decimal_rate_dataset(records_count=10):
    return {
        "fields": [
            {
                "name": "indicador_1",
                "label": "Taxa de Utilização Consultas Médicas 1 Ano (Nos Utentes sem MdF)",
                "type": "text",
            },
            {"name": "regiao", "label": "Região", "type": "text"},
        ],
        "metas": {"default": {"title": "Taxas de utilização", "records_count": records_count}},
    }


def decimal_rate_records():
    values = ["0", "21,81", "22,21", "22,67", "22,8", "24,1", "25,35", "27,02", "28,9", "31,44"]
    return {
        "results": [{"indicador_1": value, "regiao": "Norte" if index < 5 else "Centro"} for index, value in enumerate(values)],
        "total_count": len(values),
    }


def repeated_period_records():
    return {
        "results": [
            {"periodo": "2025-01", "valor": 10, "producao": 2, "regiao": "Norte"},
            {"periodo": "2025-01", "valor": 20, "producao": 3, "regiao": "Norte"},
            {"periodo": "2025-02", "valor": 30, "producao": 5, "regiao": "Centro"},
            {"periodo": "2025-02", "valor": 40, "producao": 5, "regiao": "Centro"},
            {"periodo": "2025-03", "valor": 50, "producao": 10, "regiao": "Sul"},
            {"periodo": "2025-03", "valor": 60, "producao": 10, "regiao": "Sul"},
            {"periodo": "2025-04", "valor": 70, "producao": 14, "regiao": "Norte"},
            {"periodo": "2025-04", "valor": 80, "producao": 16, "regiao": "Norte"},
        ],
        "total_count": 8,
    }


def complementary_rate_dataset(records_count=12):
    return {
        "fields": [
            {"name": "periodo", "label": "Periodo", "type": "date"},
            {"name": "com_mdf", "label": "% Total Utentes com MdF atribuído", "type": "float"},
            {"name": "sem_mdf", "label": "% Total Utentes sem MdF Atribuído", "type": "float"},
            {"name": "atividade", "label": "Atividade assistencial", "type": "integer"},
        ],
        "metas": {"default": {"title": "Taxas complementares", "records_count": records_count}},
    }


def complementary_rate_records():
    rows = []
    for month in range(1, 13):
        com_mdf = 50 + month
        rows.append(
            {
                "periodo": f"2025-{month:02d}",
                "com_mdf": com_mdf,
                "sem_mdf": 100 - com_mdf,
                "atividade": 200 + month * 7,
            }
        )
    return {"results": rows, "total_count": len(rows)}


def rate_count_same_concept_dataset(records_count=12):
    return {
        "fields": [
            {"name": "periodo", "label": "Periodo", "type": "date"},
            {"name": "total_sem_mdf", "label": "Total Utentes sem MdF Atribuído", "type": "integer"},
            {"name": "perc_sem_mdf", "label": "% Total Utentes sem MdF Atribuído", "type": "float"},
            {"name": "receitas", "label": "Receitas SNS", "type": "integer"},
        ],
        "metas": {"default": {"title": "Taxa e contagem do mesmo conceito", "records_count": records_count}},
    }


def rate_count_same_concept_records():
    rows = []
    for month in range(1, 13):
        rows.append(
            {
                "periodo": f"2025-{month:02d}",
                "total_sem_mdf": 1000 + month * 20,
                "perc_sem_mdf": 18 + month * 0.5,
                "receitas": 300 + month * 11,
            }
        )
    return {"results": rows, "total_count": len(rows)}


class AnalyticsMethodTests(unittest.TestCase):
    def setUp(self):
        server._cache.clear()

    def test_median_handles_even_samples(self):
        self.assertEqual(server._median([1, 9, 3, 7]), 5)

    def test_spearman_uses_average_ranks_for_ties(self):
        corr = server._spearman([(1, 1), (1, 1), (2, 2), (3, 3), (4, 4), (4, 4), (5, 5), (6, 6)])
        self.assertAlmostEqual(corr, 1.0, places=6)

    def test_data_analytics_returns_methodology_and_association_metadata(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return fake_records()
            return fake_dataset(records_count=120)

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            payload = server._build_data_analytics("synthetic", 80)

        self.assertEqual(payload["methodology"]["version"], server.ANALYTICS_METHOD_VERSION)
        self.assertEqual(payload["sample"]["total_records"], 120)
        self.assertTrue(payload["quality_warnings"])
        self.assertTrue(payload["correlations"])
        self.assertIn("spearman", payload["correlations"][0])
        self.assertIn("warnings", payload["correlations"][0])
        self.assertIn("analysis_readiness", payload)
        self.assertIn(payload["analysis_readiness"]["band"], {"pronto", "rever", "fragil"})
        self.assertGreaterEqual(payload["analysis_readiness"]["score"], 0)

    def test_geopoint_dimension_is_not_rendered_as_nominal_categories(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return geo_records()
            return geo_dataset(records_count=5)

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            payload = server._build_data_analytics("geo", 80)

        spatial = next(profile for profile in payload["categorical_profiles"] if profile["field"] == "localizacao")
        self.assertEqual(spatial["semantic_role"], "geolocation")
        self.assertEqual(spatial["type"], "geo")
        self.assertIn("bounds", spatial)
        self.assertEqual(spatial["top_values"], [])
        self.assertNotIn("{'lon'", str(spatial))

    def test_percent_prefixed_measure_is_rate_role(self):
        role = server._measure_role({"name": "total_utentes_sem_mdf_atribuido0", "label": "% Total Utentes sem MdF Atribuído"})
        self.assertEqual(role, "taxa")

    def test_measure_context_uses_field_and_dataset_context(self):
        money = server._measure_context(
            {"name": "valor_total", "label": "Valor total", "type": "float"},
            dataset_title="Despesa hospitalar por região",
        )
        stock = server._measure_context({"name": "valor_stock", "label": "Valor stock", "type": "integer"})
        volume = server._measure_context({"name": "saida", "label": "Quantidade", "type": "integer"})
        rate = server._measure_context({"name": "cobertura", "label": "% Cobertura", "type": "float"})

        self.assertEqual(money["role"], "monetario")
        self.assertEqual(money["unit_family"], "moeda")
        self.assertEqual(stock["role"], "stock")
        self.assertEqual(volume["role"], "contagem")
        self.assertEqual(rate["role"], "taxa")

    def test_dataset_semantic_profile_distinguishes_money_from_volume(self):
        profile = server._dataset_semantic_profile(
            [
                {"name": "periodo", "label": "Periodo", "type": "date"},
                {"name": "saida", "label": "Quantidade", "type": "integer"},
                {"name": "regiao", "label": "Região", "type": "text"},
            ],
            dataset_title="Evolução Mensal da Distribuição da Reserva Estratégica de Medicamentos",
            mega_theme="Finanças & Compras",
        )

        self.assertEqual(profile["finprod_role"], "volume")
        self.assertEqual(profile["role_counts"]["contagem"], 1)

    def test_decimal_rate_text_field_is_numeric_not_nominal_category(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return decimal_rate_records()
            return decimal_rate_dataset(records_count=10)

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            payload = server._build_data_analytics("rates", 80)

        rate_profile = next(profile for profile in payload["numeric_profiles"] if profile["field"] == "indicador_1")
        self.assertEqual(rate_profile["measure_role"], "taxa")
        self.assertEqual(rate_profile["type"], "float")
        self.assertNotIn("indicador_1", {profile["field"] for profile in payload["categorical_profiles"]})

    def test_data_analytics_filters_complementary_rate_correlations(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return complementary_rate_records()
            return complementary_rate_dataset(records_count=12)

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            payload = server._build_data_analytics("complementary", 80)

        visible_pairs = {
            frozenset((row["label_a"], row["label_b"]))
            for row in payload["correlations"]
        }
        hidden_pair = frozenset(("% Total Utentes com MdF atribuído", "% Total Utentes sem MdF Atribuído"))
        self.assertNotIn(hidden_pair, visible_pairs)
        self.assertTrue(
            any("complementares" in item["reason"] for item in payload["correlation_exclusions"])
        )

    def test_data_analytics_filters_rate_count_same_concept_correlations(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return rate_count_same_concept_records()
            return rate_count_same_concept_dataset(records_count=12)

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            payload = server._build_data_analytics("same-concept", 80)

        visible_pairs = {
            frozenset((row["label_a"], row["label_b"]))
            for row in payload["correlations"]
        }
        hidden_pair = frozenset(("Total Utentes sem MdF Atribuído", "% Total Utentes sem MdF Atribuído"))
        self.assertNotIn(hidden_pair, visible_pairs)
        self.assertTrue(
            any("taxa e contagem" in item["reason"] for item in payload["correlation_exclusions"])
        )

    def test_correlation_filters_rates_with_overlapping_population_denominators(self):
        reason = server._correlation_exclusion_reason(
            {"name": "com_mdf", "label": "% Total Utentes com MdF atribuído"},
            {"name": "utilizacao", "label": "Taxa de Utilização Consultas Médicas 1 Ano (Todos os Utentes)"},
            [(50 + index, 20 + index * 0.8) for index in range(12)],
        )

        self.assertIsNotNone(reason)
        self.assertIn("denominadores", reason)

    def test_demo_data_analytics_is_rejected(self):
        with self.assertRaises(ValueError):
            server._build_data_analytics("demo-despesa-sns", 80)

    def test_finprod_returns_methodology_and_comparability_checks(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return fake_records()
            return fake_dataset(records_count=120)

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            payload = server._build_finance_production("financial", "production", 80)

        self.assertEqual(payload["methodology"]["version"], server.ANALYTICS_METHOD_VERSION)
        self.assertGreaterEqual(len(payload["comparability"]["checks"]), 5)
        self.assertIn("required_validation", payload["methodology"])
        self.assertIn("aggregation", payload)
        self.assertIn("numerator", payload)
        self.assertIn("denominator", payload)

    def test_finprod_uses_period_sums_for_unit_cost(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return repeated_period_records()
            return fake_dataset(records_count=8)

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            payload = server._build_finance_production("financial", "production", 80)

        first = payload["rows"][0]
        self.assertEqual(first["period"], "2025-01")
        self.assertEqual(first["financial_value"], 30)
        self.assertEqual(first["production_value"], 5)
        self.assertEqual(first["unit_cost"], 6)
        self.assertEqual(payload["numerator"]["aggregation"], "soma")
        self.assertEqual(payload["denominator"]["aggregation"], "soma")

    def test_finprod_blocks_unit_cost_when_numerator_is_not_monetary(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return {
                    "results": [
                        {"periodo": "2025-01", "saida": 10, "producao": 2},
                        {"periodo": "2025-01", "saida": 20, "producao": 3},
                        {"periodo": "2025-02", "saida": 30, "producao": 5},
                        {"periodo": "2025-02", "saida": 40, "producao": 5},
                        {"periodo": "2025-03", "saida": 50, "producao": 10},
                        {"periodo": "2025-03", "saida": 60, "producao": 10},
                    ],
                    "total_count": 6,
                }
            return {
                "fields": [
                    {"name": "periodo", "label": "Periodo", "type": "date"},
                    {"name": "saida", "label": "Quantidade", "type": "integer"},
                    {"name": "producao", "label": "Produção", "type": "integer"},
                ],
                "metas": {"default": {"title": "Distribuição de stock", "records_count": 6}},
            }

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            payload = server._build_finance_production("financial", "production", 80)

        self.assertTrue(payload["summary"]["blocked"])
        self.assertEqual(payload["numerator"]["role"], "contagem")
        self.assertIsNone(payload["rows"][0]["unit_cost"])
        self.assertTrue(any("não valor monetário" in warning for warning in payload["unit_warnings"]))

    def test_finprod_role_score_matches_measure_roles(self):
        self.assertGreater(
            server._finprod_trend_role_score({"label": "Valor", "measure_role": "monetario"}, mode="financial"),
            server._finprod_trend_role_score({"label": "Valor", "measure_role": "contagem"}, mode="financial"),
        )
        self.assertGreater(
            server._finprod_trend_role_score({"label": "Produção", "measure_role": "contagem"}, mode="production"),
            server._finprod_trend_role_score({"label": "Produção", "measure_role": "monetario"}, mode="production"),
        )

    def test_finprod_selects_trend_pair_with_shared_periods(self):
        financial_trends = [
            {
                "field": "despesa",
                "label": "Despesa",
                "points": [{"period": "2023-01", "avg": 10}, {"period": "2023-02", "avg": 11}],
            },
            {
                "field": "custo_total",
                "label": "Custo total",
                "points": [{"period": "2024-01", "avg": 20}, {"period": "2024-02", "avg": 22}, {"period": "2024-03", "avg": 24}],
            },
        ]
        production_trends = [
            {
                "field": "atividade",
                "label": "Atividade",
                "points": [{"period": "2025-01", "avg": 5}, {"period": "2025-02", "avg": 6}],
            },
            {
                "field": "producao",
                "label": "Produção",
                "points": [{"period": "2024-01", "avg": 2}, {"period": "2024-02", "avg": 3}, {"period": "2024-03", "avg": 4}],
            },
        ]

        selected = server._select_finprod_trend_pair(financial_trends, production_trends)

        self.assertEqual(selected["financial_trend"]["field"], "custo_total")
        self.assertEqual(selected["production_trend"]["field"], "producao")
        self.assertEqual(len(selected["shared_periods"]), 3)
        self.assertEqual(selected["selection_reason"], "melhor_sobreposicao_temporal")

    def test_finprod_recommendations_rank_by_temporal_overlap(self):
        catalog = {
            "datasets": [
                {
                    "dataset_id": "fin",
                    "title": "Financeiro",
                    "mega_theme": "Finanças & Compras",
                    "metric_candidate_count": 2,
                    "records_count": 100,
                },
                {
                    "dataset_id": "prod-good",
                    "title": "Produção boa",
                    "mega_theme": "Acesso & Produção",
                    "metric_candidate_count": 2,
                    "records_count": 100,
                },
                {
                    "dataset_id": "prod-weak",
                    "title": "Produção fraca",
                    "mega_theme": "Acesso & Produção",
                    "metric_candidate_count": 4,
                    "records_count": 200,
                },
            ]
        }

        def fake_finprod(financial_dataset, production_dataset, limit):
            matched = 8 if production_dataset == "prod-good" else 1
            return {
                "financial_dataset": {"trend_label": "Despesa"},
                "production_dataset": {"trend_label": "Produção"},
                "summary": {
                    "matched_periods": matched,
                    "sample_pairs": matched,
                    "robustness": "moderada" if matched >= 8 else "insuficiente",
                    "correlation_strength": "moderada" if matched >= 8 else "insuficiente",
                },
                "diagnostics": {
                    "trend_candidates": [
                        {
                            "financial_range": {"start": "2024-01", "end": "2024-08"},
                            "production_range": {"start": "2024-01", "end": "2024-08"},
                        }
                    ]
                },
            }

        with patch.object(server, "_get_analysis_catalog", return_value=catalog):
            with patch.object(server, "_build_finance_production", side_effect=fake_finprod):
                payload = server._build_finprod_recommendations("fin", "prod-weak", 80, 4)

        self.assertEqual(payload["recommendations"][0]["production_dataset_id"], "prod-good")
        self.assertEqual(payload["recommendations"][0]["matched_periods"], 8)
        self.assertGreaterEqual(len(payload["candidates"]), len(payload["recommendations"]))

    def test_finprod_recommendations_filters_zero_overlap(self):
        catalog = {
            "datasets": [
                {"dataset_id": "fin", "title": "Financeiro", "mega_theme": "Finanças & Compras"},
                {"dataset_id": "prod-zero", "title": "Produção zero", "mega_theme": "Acesso & Produção"},
            ]
        }

        def fake_finprod(financial_dataset, production_dataset, limit):
            return {
                "financial_dataset": {"trend_label": "Despesa"},
                "production_dataset": {"trend_label": "Produção"},
                "summary": {"matched_periods": 0, "sample_pairs": 0, "robustness": "insuficiente", "correlation_strength": "insuficiente"},
                "diagnostics": {"trend_candidates": []},
            }

        with patch.object(server, "_get_analysis_catalog", return_value=catalog):
            with patch.object(server, "_build_finance_production", side_effect=fake_finprod):
                payload = server._build_finprod_recommendations("fin", None, 80, 4)

        self.assertEqual(payload["recommendations"], [])
        self.assertEqual(payload["candidates"][0]["production_dataset_id"], "prod-zero")
        self.assertEqual(payload["candidates"][0]["matched_periods"], 0)
        self.assertEqual(payload["useful_count"], 0)
        self.assertIn("Sem alternativas", payload["warning"])

    def test_predictive_recommendations_identify_ready_and_near_datasets(self):
        catalog = {
            "datasets": [
                {"dataset_id": "active", "title": "Taxa sazonal ativa", "metric_candidate_count": 2, "field_count": 8, "records_count": 21},
                {"dataset_id": "ready", "title": "Evolução mensal pronta", "metric_candidate_count": 3, "field_count": 8, "records_count": 96},
            ]
        }

        def trend(label, periods):
            return {
                "label": label,
                "field": "valor",
                "points": [{"period": f"2025-{index:02d}", "value": index * 10} for index in range(1, periods + 1)],
            }

        def fake_data_analytics(dataset_id, limit):
            if dataset_id == "ready":
                return {
                    "dataset_id": "ready",
                    "title": "Evolução mensal pronta",
                    "sample_size": 96,
                    "total_records": 96,
                    "temporal_field": "periodo",
                    "trends": [trend("Valor mensal", 8)],
                }
            return {
                "dataset_id": "active",
                "title": "Taxa sazonal ativa",
                "sample_size": 21,
                "total_records": 21,
                "temporal_field": "periodo",
                "trends": [trend("Taxa sazonal", 21)],
            }

        with patch.object(server, "_get_analysis_catalog", return_value=catalog):
            with patch.object(server, "_build_data_analytics", side_effect=fake_data_analytics):
                payload = server._build_predictive_recommendations("active", 80, 4)

        self.assertEqual(payload["active"]["band"], "near")
        self.assertEqual(payload["recommendations"][0]["dataset_id"], "ready")
        self.assertIn(payload["recommendations"][0]["band"], {"ready", "usable"})
        self.assertGreaterEqual(payload["ready_count"], 1)

    def test_feature_screening_is_deterministic_and_copy_safe(self):
        payload = {
            "dataset_id": "demo",
            "feature_importance": [
                {"field": "valor", "label": "Valor", "kind": "medida", "score": 90, "drivers": ["a"]},
                {"field": "regiao", "label": "Região", "kind": "dimensao", "score": 20, "drivers": ["b"]},
            ],
        }
        original = [dict(item) for item in payload["feature_importance"]]

        first = server._build_boruta_selection(payload)
        second = server._build_boruta_selection(payload)

        self.assertEqual(first, second)
        self.assertEqual(payload["feature_importance"], original)
        self.assertEqual(first["method"], "feature_screening_deterministic_shadow")

    def test_analysis_readiness_caps_missingness_and_missing_time(self):
        readiness = server._analysis_readiness(
            sample_size=100,
            total_records=100,
            temporal_field=None,
            numeric_count=5,
            categorical_count=5,
            correlation_count=10,
            trend_count=3,
            warning_count=0,
            max_missing_ratio=0.5,
        )

        self.assertLessEqual(readiness["score"], 68)
        self.assertIn("tempo", readiness["gaps"])
        self.assertIn("missing", readiness["gaps"])


if __name__ == "__main__":
    unittest.main()
