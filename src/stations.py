"""
Station registry for the EV Charging Agentic Intelligence project.

Generates 120 charging stations distributed across 8 major German cities,
with realistic connector type distribution and geographic jitter so stations
appear as a dense local network on the map (not a single dot per city).
"""
import numpy as np
import pandas as pd

np.random.seed(42)

# Real city centers (lat, lon)
CITIES = {
    "BER": {"name": "Berlin",     "lat": 52.5200, "lon": 13.4050, "stations": 18},
    "MUC": {"name": "Munich",     "lat": 48.1351, "lon": 11.5820, "stations": 16},
    "HAM": {"name": "Hamburg",    "lat": 53.5511, "lon": 9.9937,  "stations": 15},
    "FRA": {"name": "Frankfurt",  "lat": 50.1109, "lon": 8.6821,  "stations": 14},
    "CGN": {"name": "Cologne",    "lat": 50.9375, "lon": 6.9603,  "stations": 13},
    "STU": {"name": "Stuttgart",  "lat": 48.7758, "lon": 9.1829,  "stations": 12},
    "DUS": {"name": "Düsseldorf", "lat": 51.2277, "lon": 6.7735,  "stations": 11},
    "LEJ": {"name": "Leipzig",    "lat": 51.3397, "lon": 12.3731, "stations": 11},
}

# Connector type distribution: realistic mix — CCS2 dominant (modern), some
# CHAdeMO (legacy/Japanese OEMs), Type2 (slower AC, often workplace/retail)
CONNECTOR_SPECS = {
    "CCS2":    {"max_power_kw": 150, "nominal_voltage": 400, "weight": 0.50},
    "CHAdeMO": {"max_power_kw": 50,  "nominal_voltage": 400, "weight": 0.15},
    "Type2":   {"max_power_kw": 22,  "nominal_voltage": 400, "weight": 0.35},
}

FIRMWARE_VERSIONS = ["2.4.1", "2.4.1", "2.4.1", "2.3.0"]  # 25% on older firmware


def generate_stations() -> pd.DataFrame:
    """
    Generates the static station registry: ID, location, connector type,
    firmware version. Stations are jittered around city centers (~3-6km
    spread) to form a realistic dense local network on the map.
    """
    rows = []
    connector_types = list(CONNECTOR_SPECS.keys())
    connector_weights = [CONNECTOR_SPECS[c]["weight"] for c in connector_types]

    for city_code, city in CITIES.items():
        n = city["stations"]
        for i in range(n):
            station_id = f"DE-{city_code}-{i+1:04d}"

            # Geographic jitter: ~0.01-0.05 deg ~ 1-5km spread around city center
            lat_jitter = np.random.normal(0, 0.025)
            lon_jitter = np.random.normal(0, 0.025)

            connector = np.random.choice(connector_types, p=connector_weights)
            firmware = np.random.choice(FIRMWARE_VERSIONS)

            rows.append({
                "station_id": station_id,
                "city_code": city_code,
                "city_name": city["name"],
                "lat": round(city["lat"] + lat_jitter, 6),
                "lon": round(city["lon"] + lon_jitter, 6),
                "connector_type": connector,
                "max_power_kw": CONNECTOR_SPECS[connector]["max_power_kw"],
                "nominal_voltage": CONNECTOR_SPECS[connector]["nominal_voltage"],
                "firmware_version": firmware,
            })

    return pd.DataFrame(rows)


if __name__ == "__main__":
    stations = generate_stations()
    stations.to_csv("data/stations.csv", index=False)
    print(f"Generated {len(stations)} stations across {len(CITIES)} cities")
    print(stations["city_name"].value_counts())
    print("\nConnector distribution:")
    print(stations["connector_type"].value_counts())
    print("\nFirmware distribution:")
    print(stations["firmware_version"].value_counts())
    print(f"\n-> data/stations.csv")