from pathlib import Path

from app.services.aisstream import normalize_message


def test_ais_position_normalization() -> None:
    row = normalize_message(
        {
            "Metadata": {
                "MMSI": 316001245,
                "ShipName": "TEST VESSEL",
                "Latitude": 45.5,
                "Longitude": -73.6,
            },
            "Message": {
                "PositionReport": {
                    "Sog": 12.4,
                    "Cog": 91.2,
                    "TrueHeading": 90,
                }
            },
        }
    )
    assert row is not None
    assert row["id"] == "mmsi_316001245"
    assert row["source"] == "aisstream"
    assert row["lat"] == 45.5


def test_ais_rejects_invalid_coordinates() -> None:
    assert normalize_message(
        {"Metadata": {"MMSI": 1, "Latitude": 120, "Longitude": 0}}
    ) is None


def test_no_simulated_geospatial_fallbacks_remain() -> None:
    services = Path(__file__).resolve().parents[1] / "app" / "services"
    for filename in ("opensky.py", "celestrak.py"):
        source = (services / filename).read_text(encoding="utf-8").lower()
        assert "simulated_fallback" not in source
        assert "static_fallback" not in source
