import unittest
from unittest.mock import patch

import server


class FacetClassificationTests(unittest.TestCase):
    def test_classifies_local_and_institution_facets(self):
        facets = server._classify_facets(
            "atividade-uls-hospitalar",
            "Produção por ULS e hospital",
            ["periodo", "regiao", "uls", "hospital", "concelho", "entidade", "consultas"],
        )

        self.assertIn("uls", facets["tags"])
        self.assertIn("regiao", facets["tags"])
        self.assertIn("hospital", facets["tags"])
        self.assertIn("concelho", facets["tags"])
        self.assertIn("instituicao", facets["tags"])
        self.assertIn("uls", facets["institution_types"])
        self.assertIn("hospital", facets["institution_types"])
        self.assertIn("territorial", facets["dimension_types"])
        self.assertIn("entidade", facets["dimension_types"])

    def test_classifies_uf_and_clinical_problem_facets(self):
        facets = server._classify_facets(
            "problemas-icpc-uf",
            "Problemas ICPC por unidade funcional",
            ["periodo", "unidade_funcional", "icpc_codigo", "problema_ativo", "utente", "valor"],
        )

        self.assertIn("uf", facets["tags"])
        self.assertIn("icpc", facets["tags"])
        self.assertIn("uf", facets["local_scopes"])
        self.assertIn("uf", facets["institution_types"])
        self.assertIn("clinico", facets["dimension_types"])
        self.assertIn("entidade", facets["dimension_types"])

    def test_analysis_payload_includes_facet_counts(self):
        payload = {
            "results": [
                {
                    "dataset_id": "sample-uls-regiao",
                    "title": "Indicadores por ULS e Região",
                    "fields": [
                        {"name": "periodo", "label": "Período"},
                        {"name": "uls", "label": "ULS"},
                        {"name": "regiao", "label": "Região"},
                        {"name": "valor", "label": "Valor"},
                    ],
                },
                {
                    "dataset_id": "sample-hospital-entidade",
                    "title": "Despesa por Hospital e Entidade",
                    "fields": [
                        {"name": "periodo", "label": "Período"},
                        {"name": "hospital", "label": "Hospital"},
                        {"name": "entidade", "label": "Entidade"},
                        {"name": "custo", "label": "Custo"},
                    ],
                },
            ]
        }

        analysis = server._analyze_datasets(payload)

        self.assertIn("facet_counts", analysis)
        self.assertGreaterEqual(analysis["facet_counts"]["tags"].get("uls", 0), 1)
        self.assertGreaterEqual(analysis["facet_counts"]["tags"].get("hospital", 0), 1)
        self.assertIn("facets", analysis["datasets"][0])
        self.assertIn("quality_flags", analysis["datasets"][0])
        self.assertIn("analysis_readiness", analysis["datasets"][0])
        self.assertIn(analysis["datasets"][0]["analysis_readiness"]["band"], {"pronto", "rever", "fragil"})
        self.assertIsInstance(analysis["datasets"][0]["analysis_readiness"]["gaps"], list)

    def test_catalog_fetch_preserves_metadata_shape_without_select(self):
        calls = []

        def mock_fetch(path, params=None, cacheable=True):
            calls.append((path, params or {}))
            return {
                "total_count": 1,
                "results": [
                    {
                        "dataset_id": "sample",
                        "title": "Amostra",
                        "records_count": 10,
                        "metas": {"default": {"records_count": 10}},
                        "fields": [{"name": "periodo"}],
                    }
                ],
            }

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            payload = server._get_datasets()

        self.assertEqual(payload["results"][0]["metas"]["default"]["records_count"], 10)
        self.assertNotIn("select", calls[0][1])
        self.assertEqual(calls[0][1]["limit"], str(server.MAX_DATASET_LIMIT))


if __name__ == "__main__":
    unittest.main()
