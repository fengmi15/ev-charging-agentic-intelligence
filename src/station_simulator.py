"""
EV Charging Network — Station Telemetry Simulator

Simulates 24 hours of 1-minute-resolution telemetry for ~120 charging
stations across 8 German cities. Each station runs an independent session
state machine (IDLE -> CHARGING -> IDLE) with realistic CC-CV charging
curves, time-of-day-dependent arrival rates, and per-city grid load
aggregation.

This produces the "clean" baseline dataset. Fault injection happens in a
separate pass (fault_injector.py) so ground-truth labels stay available
for evaluating the anomaly-detection agents later.
"""
import numpy as np
import pandas as pd
from stations import generate_stations, CONNECTOR_SPECS

np.random.seed(123)

MINUTES_PER_DAY = 24 * 60


def session_arrival_probability(hour: int) -> float:
    """
    Hourly probability (0-1) that an IDLE station starts a new charging
    session, shaped like a realistic urban demand curve:
    - Overnight low (depot/home charging dominates, public stations quiet)
    - Morning peak ~07:00-09:00 (commuters topping up before work)
    - Midday moderate (errands, lunch breaks)
    - Evening peak ~17:00-20:00 (commute home, shopping)
    """
    profile = {
        0: 0.01, 1: 0.01, 2: 0.01, 3: 0.01, 4: 0.01, 5: 0.02,
        6: 0.05, 7: 0.12, 8: 0.15, 9: 0.10, 10: 0.07, 11: 0.06,
        12: 0.08, 13: 0.07, 14: 0.06, 15: 0.07, 16: 0.09, 17: 0.14,
        18: 0.16, 19: 0.13, 20: 0.08, 21: 0.05, 22: 0.03, 23: 0.02,
    }
    return profile[hour]


def ambient_temperature(hour: int, minute_frac: float) -> float:
    """
    Simple daily ambient temperature sinusoid: low ~6C overnight,
    peak ~19C around 15:00. Shared across all cities for simplicity
    (could be per-city offset later).
    """
    t = hour + minute_frac
    return 12.5 + 6.5 * np.sin((t - 9) / 24 * 2 * np.pi)


class StationState:
    """Mutable per-station simulation state."""
    def __init__(self, station_row):
        self.station_id = station_row["station_id"]
        self.city_code = station_row["city_code"]
        self.connector_type = station_row["connector_type"]
        self.max_power_kw = station_row["max_power_kw"]
        self.nominal_voltage = station_row["nominal_voltage"]
        self.firmware_version = station_row["firmware_version"]

        self.status = "IDLE"
        self.session_id = None
        self.session_counter = 0
        self.soc_pct = 0.0
        self.target_soc_pct = 0.0
        self.battery_capacity_kwh = 0.0
        self.energy_delivered_kwh = 0.0
        self.connector_temp_c = 15.0  # starts near ambient

    def maybe_start_session(self, hour: int):
        if self.status != "IDLE":
            return
        if np.random.random() < session_arrival_probability(hour):
            self.status = "CHARGING"
            self.session_counter += 1
            self.session_id = f"{self.station_id}-S{self.session_counter:04d}"
            self.battery_capacity_kwh = np.random.uniform(40, 100)
            self.soc_pct = np.random.uniform(8, 45)
            self.target_soc_pct = np.random.uniform(80, 100)
            self.energy_delivered_kwh = 0.0

    def step_charging(self, ambient_temp: float):
        """
        CC-CV charging curve:
        - CC phase (soc < 80%): power ~ max_power_kw with small noise
        - CV phase (soc >= 80%): power tapers linearly toward ~10% of max
          as soc approaches target_soc
        """
        if self.soc_pct < 80:
            power_kw = self.max_power_kw * np.random.uniform(0.92, 1.0)
        else:
            # taper fraction: 1.0 at soc=80, ~0.1 at soc=target_soc
            span = max(self.target_soc_pct - 80, 1e-3)
            frac = np.clip((self.target_soc_pct - self.soc_pct) / span, 0.05, 1.0)
            power_kw = self.max_power_kw * (0.1 + 0.9 * frac) * np.random.uniform(0.95, 1.0)

        power_kw = max(power_kw, 0.5)

        # SoC increase this minute
        delta_soc = (power_kw * (1/60) / self.battery_capacity_kwh) * 100
        self.soc_pct = min(self.soc_pct + delta_soc, 100.0)
        self.energy_delivered_kwh += power_kw / 60

        # Connector heating: load-proportional heating, asymptotic toward
        # a load-dependent steady state, plus ambient coupling
        steady_state_temp = ambient_temp + (power_kw / self.max_power_kw) * 35
        self.connector_temp_c += (steady_state_temp - self.connector_temp_c) * 0.08
        self.connector_temp_c += np.random.normal(0, 0.3)

        current_a = (power_kw * 1000) / self.nominal_voltage
        voltage_v = self.nominal_voltage + np.random.normal(0, 1.5)

        # Session ends when target reached
        if self.soc_pct >= self.target_soc_pct:
            self.status = "IDLE"
            session_id = self.session_id
            self.session_id = None
            return power_kw, voltage_v, current_a, session_id

        return power_kw, voltage_v, current_a, self.session_id

    def step_idle(self, ambient_temp: float):
        # Connector cools toward ambient
        self.connector_temp_c += (ambient_temp - self.connector_temp_c) * 0.05
        self.connector_temp_c += np.random.normal(0, 0.2)
        return 0.0, self.nominal_voltage + np.random.normal(0, 1.0), 0.0, None


def simulate_day(stations_df: pd.DataFrame, freq_minutes: int = 1) -> pd.DataFrame:
    """
    Runs the full 24-hour simulation across all stations.
    Returns a flat DataFrame: one row per (station, timestamp).
    """
    states = [StationState(row) for _, row in stations_df.iterrows()]
    n_steps = MINUTES_PER_DAY // freq_minutes

    records = []
    for step in range(n_steps):
        minute_of_day = step * freq_minutes
        hour = (minute_of_day // 60) % 24
        minute_frac = (minute_of_day % 60) / 60
        ambient = ambient_temperature(hour, minute_frac)
        timestamp = step * freq_minutes * 60  # seconds since midnight

        for st in states:
            st.maybe_start_session(hour)

            if st.status == "CHARGING":
                power, voltage, current, session_id = st.step_charging(ambient)
                # session_id is the ID of the session that was active THIS minute,
                # even if it completed during this step (st.status may now be IDLE)
                status_out = "CHARGING" if session_id else "IDLE"
            else:
                power, voltage, current, session_id = st.step_idle(ambient)
                status_out = "IDLE"

            records.append({
                "station_id": st.station_id,
                "city_code": st.city_code,
                "connector_type": st.connector_type,
                "firmware_version": st.firmware_version,
                "timestamp_s": timestamp,
                "status": status_out,
                "session_id": session_id,
                "soc_pct": round(st.soc_pct, 2) if session_id else None,
                "voltage_v": round(voltage, 2),
                "current_a": round(current, 2),
                "power_kw": round(power, 3),
                "energy_delivered_kwh": round(st.energy_delivered_kwh, 3) if session_id else None,
                "connector_temp_c": round(st.connector_temp_c, 2),
                "ambient_temp_c": round(ambient, 2),
                "fault_code": None,
            })

    return pd.DataFrame(records)


def compute_grid_load(telemetry: pd.DataFrame, stations_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates per-city power draw at each timestamp and expresses it as a
    percentage of that city's notional grid capacity. Grid capacity is set
    so that ~60% of stations charging simultaneously at full power would
    hit ~100% load -- realistic congestion dynamics.
    """
    city_capacity = (
        stations_df.groupby("city_code")["max_power_kw"].sum() * 0.6
    ).to_dict()

    city_load = (
        telemetry.groupby(["city_code", "timestamp_s"])["power_kw"]
        .sum()
        .reset_index()
        .rename(columns={"power_kw": "city_total_power_kw"})
    )
    city_load["grid_capacity_kw"] = city_load["city_code"].map(city_capacity)
    city_load["grid_load_pct"] = round(
        100 * city_load["city_total_power_kw"] / city_load["grid_capacity_kw"], 2
    )

    telemetry = telemetry.merge(
        city_load[["city_code", "timestamp_s", "grid_load_pct"]],
        on=["city_code", "timestamp_s"], how="left"
    )
    return telemetry


if __name__ == "__main__":
    print("Generating station registry...")
    stations_df = generate_stations()
    stations_df.to_csv("data/stations.csv", index=False)
    print(f"  {len(stations_df)} stations across {stations_df['city_code'].nunique()} cities")

    print("\nSimulating 24h telemetry at 1-min resolution...")
    telemetry = simulate_day(stations_df, freq_minutes=1)
    print(f"  {len(telemetry):,} rows generated")

    print("\nComputing per-city grid load...")
    telemetry = compute_grid_load(telemetry, stations_df)

    telemetry.to_csv("data/telemetry_raw.csv", index=False)
    print(f"\n-> data/telemetry_raw.csv ({len(telemetry):,} rows, {telemetry['station_id'].nunique()} stations)")

    # Quick sanity stats
    print("\nStatus distribution:")
    print(telemetry["status"].value_counts())
    print(f"\nTotal sessions started: {telemetry['session_id'].nunique()}")
    print(f"Avg connector temp: {telemetry['connector_temp_c'].mean():.1f}C")
    print(f"Max grid load observed: {telemetry['grid_load_pct'].max():.1f}%")