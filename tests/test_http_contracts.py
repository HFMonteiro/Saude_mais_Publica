import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import urlopen
from unittest.mock import patch

import server
from test_analytics_methods import fake_dataset, fake_records


class HttpContractTests(unittest.TestCase):
    def setUp(self):
        server._cache.clear()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.TransparenciaHandler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def tearDown(self):
        self.httpd.shutdown()
        self.thread.join(timeout=5)
        self.httpd.server_close()

    def get_json(self, path):
        with urlopen(f"{self.base_url}{path}", timeout=5) as response:
            return response.status, dict(response.headers), json.loads(response.read().decode("utf-8"))

    def test_health_contract(self):
        status, headers, payload = self.get_json("/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"status": "ok"})
        self.assertIn("no-store", headers.get("Cache-Control", ""))

    def test_status_reports_contract_errors_without_claiming_outage(self):
        error = server.UpstreamContractError(
            "ODS HTTP 400 on /catalog/datasets",
            status_code=400,
            path="/catalog/datasets",
        )
        with patch.object(server, "_get_analysis_catalog", side_effect=error):
            status, headers, payload = self.get_json("/api/status")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "degraded")
        self.assertIn("no-store", headers.get("Cache-Control", ""))
        self.assertTrue(payload["catalog"]["fallback"])
        self.assertEqual(payload["catalog"]["error_kind"], "upstream_contract_error")
        self.assertEqual(payload["catalog"]["upstream_status"], 400)
        self.assertIn("configuração", payload["catalog"]["warning"])

    def test_static_pages_and_assets_open(self):
        for path in [
            "/",
            "/index.html",
            "/analytics.html",
            "/crosswalk.html",
            "/metodologia.html",
            "/research.html",
            "/app.js",
            "/analytics.js",
            "/crosswalk.js",
            "/research.js",
            "/styles.css",
        ]:
            with self.subTest(path=path):
                with urlopen(f"{self.base_url}{path}", timeout=5) as response:
                    body = response.read()
                self.assertEqual(response.status, 200)
                self.assertGreater(len(body), 100)

    def test_data_analytics_contract(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return fake_records()
            return fake_dataset(records_count=120)

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            status, _, payload = self.get_json("/api/data-analytics?dataset_id=synthetic&limit=80")

        self.assertEqual(status, 200)
        self.assertEqual(payload["dataset_id"], "synthetic")
        self.assertIn("methodology", payload)
        self.assertIn("sample", payload)
        self.assertLessEqual(len(payload["correlations"]), 20)

    def test_deep_research_contract_does_not_mutate_data_analytics_cache(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return fake_records()
            return fake_dataset(records_count=120)

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            status, _, payload = self.get_json("/api/deep-research?dataset_id=synthetic&limit=80")
            _, _, data_payload = self.get_json("/api/data-analytics?dataset_id=synthetic&limit=80")

        self.assertEqual(status, 200)
        self.assertEqual(payload["dataset_id"], "synthetic")
        self.assertIn("analysis", payload)
        self.assertIn("feature_screening", payload)
        self.assertIn("territorial_map", payload)
        self.assertIn("methodology", payload)
        self.assertNotIn("feature_screening", data_payload)
        self.assertNotIn("boruta", data_payload)

    def test_deep_research_rejects_demo_dataset(self):
        with patch.object(server, "_ods_fetch", side_effect=RuntimeError("offline")):
            with self.assertRaises(HTTPError) as ctx:
                self.get_json("/api/deep-research?dataset_id=demo-despesa-sns&limit=80")

        status = ctx.exception.code
        payload = json.loads(ctx.exception.read().decode("utf-8"))
        self.assertEqual(status, 400)
        self.assertIn("Dataset demonstrativo recusado", payload["error"])

    def test_finprod_contract(self):
        def mock_fetch(path, params=None, cacheable=True):
            if path.endswith("/records"):
                return fake_records()
            return fake_dataset(records_count=120)

        with patch.object(server, "_ods_fetch", side_effect=mock_fetch):
            status, _, payload = self.get_json("/api/finprod?financial_dataset=fin&production_dataset=prod&limit=80")

        self.assertEqual(status, 200)
        self.assertIn("summary", payload)
        self.assertIn("comparability", payload)
        self.assertIn("methodology", payload)

    def test_predictive_recommendations_contract(self):
        stub = {
            "active_dataset_id": "synthetic",
            "active": None,
            "recommendations": [],
            "ready_count": 0,
            "near_count": 0,
            "blocked_count": 0,
            "candidate_count": 0,
            "errors": [],
            "methodology": {"version": server.ANALYTICS_METHOD_VERSION},
        }
        with patch.object(server, "_build_predictive_recommendations", return_value=stub):
            status, _, payload = self.get_json("/api/predictive/recommendations?dataset_id=synthetic&limit=80")

        self.assertEqual(status, 200)
        self.assertIn("recommendations", payload)
        self.assertIn("methodology", payload)

    def test_analysis_uses_empty_fallback_when_upstream_fails(self):
        with patch.object(server, "_get_analysis_catalog", side_effect=RuntimeError("offline")):
            status, headers, payload = self.get_json("/api/analysis?min_score=4")

        self.assertEqual(status, 200)
        self.assertTrue(payload["fallback"])
        self.assertIn("no-store", headers.get("Cache-Control", ""))
        self.assertEqual(payload["datasets"], [])
        self.assertEqual(payload["links"], [])
        self.assertIn("facet_counts", payload)
        self.assertIn("Falha ao construir catálogo", payload["warning"])
        self.assertEqual(payload["empty_reason"], "api_unavailable")

    def test_analytics_propagates_empty_fallback_metadata(self):
        with patch.object(server, "_get_analysis_catalog", side_effect=RuntimeError("offline")):
            status, headers, payload = self.get_json("/api/analytics?min_score=4")

        self.assertEqual(status, 200)
        self.assertTrue(payload["fallback"])
        self.assertIn("no-store", headers.get("Cache-Control", ""))
        self.assertIn("Falha ao construir catálogo", payload["warning"])
        self.assertIn("error_kind", payload)
        self.assertEqual(payload["methodology"]["version"], server.ANALYTICS_METHOD_VERSION)

    def test_analysis_contract_error_fallback_is_diagnostic(self):
        error = server.UpstreamContractError(
            "ODS HTTP 400 on /catalog/datasets",
            status_code=400,
            path="/catalog/datasets",
        )
        with patch.object(server, "_get_analysis_catalog", side_effect=error):
            status, _, payload = self.get_json("/api/analysis?min_score=4")

        self.assertEqual(status, 200)
        self.assertTrue(payload["fallback"])
        self.assertEqual(payload["error_kind"], "upstream_contract_error")
        self.assertEqual(payload["upstream_status"], 400)
        self.assertIn("configuração", payload["warning"])

    def test_bad_limit_returns_400_without_traceback(self):
        with self.assertRaises(HTTPError) as ctx:
            urlopen(f"{self.base_url}/api/data-analytics?dataset_id=synthetic&limit=1000", timeout=5)
        self.assertEqual(ctx.exception.code, 400)
        body = ctx.exception.read().decode("utf-8")
        self.assertIn("Invalid limit", body)
        self.assertNotIn("Traceback", body)

    def test_unknown_api_route_returns_404_json(self):
        with self.assertRaises(HTTPError) as ctx:
            urlopen(f"{self.base_url}/api/not-real", timeout=5)
        self.assertEqual(ctx.exception.code, 404)
        body = json.loads(ctx.exception.read().decode("utf-8"))
        self.assertIn("Unknown API route", body["error"])


if __name__ == "__main__":
    unittest.main()
