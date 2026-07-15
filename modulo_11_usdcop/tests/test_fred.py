import unittest

from usdcop.data.fred import FredClient


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.last_request = None

    def get(self, url, **kwargs):
        self.last_request = (url, kwargs)
        return FakeResponse(self.payload)


class FredClientTests(unittest.TestCase):
    def test_parses_json_observations_and_drops_missing_values(self):
        client = FredClient(api_key="test-key")
        client.session = FakeSession(
            {
                "observations": [
                    {"date": "2026-07-13", "value": "4.35"},
                    {"date": "2026-07-14", "value": "."},
                ]
            }
        )

        frame = client.fetch_series("SOFR")

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["value"], 4.35)
        self.assertEqual(frame.iloc[0]["series_id"], "SOFR")
        self.assertEqual(client.session.last_request[1]["params"]["api_key"], "test-key")

    def test_requires_api_key(self):
        client = FredClient(api_key=None)
        client.api_key = None
        with self.assertRaisesRegex(RuntimeError, "FRED_API_KEY"):
            client.fetch_series("SOFR")

    def test_rejects_empty_observations(self):
        client = FredClient(api_key="test-key")
        client.session = FakeSession({"observations": []})
        with self.assertRaisesRegex(ValueError, "no observations"):
            client.fetch_series("SOFR")


if __name__ == "__main__":
    unittest.main()
