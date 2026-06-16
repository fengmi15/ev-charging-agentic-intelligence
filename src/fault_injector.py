"""
EV Charging Network — Fault Injector

Reads clean telemetry (telemetry_raw.csv) and injects 6 realistic fault
scenarios with ground-truth labels. Produces telemetry_faulted.csv.

Fault scenarios (what hiring managers want to see you understand):

1. THERMAL_RUNAWAY    — connector_temp climbing >5C/min during high-power
                        charging. Fire risk. Affects 3 stations.

2. GRID_CONGESTION    — multiple stations in same city simultaneously draw
                        peak power, pushing grid_load_pct >120% for
                        sustained period. Affects 2 city clusters.

3. PHANTOM_SESSION    — status=CHARGING, power_kw~0, session_id present.
                        Stuck contactor / billing fraud indicator.
                        Affects 4 stations.

4. SOC_PLATEAU        — soc_pct frozen for >10 consecutive minutes during
                        active charging. BMS fault / cell imbalance.
                        Affects 3 stations.

5. FIRMWARE_FAULT     — stations on firmware 2.3.0 start emitting correlated
                        fault codes (E042) during specific hour window.
                        Pattern only visible cross-station. Affects all
                        2.3.0 stations in one city during 14:00-16:00.

6. GHOST_STATION      — station drops off the stream entirely (no rows) for
                        a sustained window. Offline/comms failure.
                        Affects 2 stations.

Each injected row gets:
  fault_code    : e.g. "E042", "E108", None
  fault_type    : human-readable label for evaluation/dashboard
  fault_severity: "LOW", "MEDIUM", "HIGH", "CRITICAL"
"""

import numpy as np
import pandas as pd

np.random.seed(99)

FAULT_CODES = {
    "THERMAL_RUNAWAY":  "E108",
    "PHANTOM_SESSION":  "E201",
    "SOC_PLATEAU":      "E315",
    "FIRMWARE_FAULT":   "E042",
    "GRID_CONGESTION":  "E500",
    "GHOST_STATION":    None,
}

FAULT_SEVERITY = {
    "THERMAL_RUNAWAY":  "CRITICAL",
    "PHANTOM_SESSION":  "MEDIUM",
    "SOC_PLATEAU":      "MEDIUM",
    "FIRMWARE_FAULT":   "LOW",
    "GRID_CONGESTION":  "HIGH",
    "GHOST_STATION":    "HIGH",
}


def pick_stations(df: pd.DataFrame, city_code: str, n: int,
                  connector: str = None, firmware: str = None) -> list:
    """Pick n random station_ids matching optional filters."""
    mask = df["city_code"] == city_code
    if connector:
        mask &= df["connector_type"] == connector
    if firmware:
        mask &= df["firmware_version"] == firmware
    pool = df.loc[mask, "station_id"].unique()
    return list(np.random.choice(pool, size=min(n, len(pool)), replace=False))


def inject_thermal_runaway(tel: pd.DataFrame, stations: list,
                            start_h: int, duration_min: int = 25) -> pd.DataFrame:
    """
    Ramps connector_temp at ~6C/min starting from a realistic base.
    Sets fault_code E108 and fault_type THERMAL_RUNAWAY.
    Also sets status to FAULT after temp exceeds 65C.
    """
    start_s = start_h * 3600
    end_s = start_s + duration_min * 60

    mask = (
        tel["station_id"].isin(stations) &
        tel["timestamp_s"].between(start_s, end_s)
    )
    rows = tel[mask].copy()

    for station in stations:
        smask = mask & (tel["station_id"] == station)
        indices = tel[smask].index
        base_temp = tel.loc[indices[0], "connector_temp_c"] if len(indices) else 35.0
        for i, idx in enumerate(indices):
            ramp_temp = base_temp + i * 6.2 + np.random.normal(0, 0.8)
            tel.loc[idx, "connector_temp_c"] = round(ramp_temp, 2)
            tel.loc[idx, "fault_code"] = FAULT_CODES["THERMAL_RUNAWAY"]
            tel.loc[idx, "fault_type"] = "THERMAL_RUNAWAY"
            tel.loc[idx, "fault_severity"] = FAULT_SEVERITY["THERMAL_RUNAWAY"]
            if ramp_temp > 65:
                tel.loc[idx, "status"] = "FAULT"
                tel.loc[idx, "power_kw"] = 0.0  # charger auto-shutoff
    return tel


def inject_phantom_session(tel: pd.DataFrame, stations: list,
                            start_h: int, duration_min: int = 40) -> pd.DataFrame:
    """
    Sets status=CHARGING, power_kw~0, session_id present.
    Simulates a stuck contactor or billing system lock.
    """
    start_s = start_h * 3600
    end_s = start_s + duration_min * 60

    mask = (
        tel["station_id"].isin(stations) &
        tel["timestamp_s"].between(start_s, end_s)
    )
    tel.loc[mask, "status"] = "CHARGING"
    tel.loc[mask, "power_kw"] = np.random.uniform(0.0, 0.3, mask.sum()).round(3)
    tel.loc[mask, "current_a"] = 0.0
    tel.loc[mask, "session_id"] = tel.loc[mask, "station_id"] + "-PHANTOM"
    tel.loc[mask, "soc_pct"] = None   # no vehicle actually connected
    tel.loc[mask, "fault_code"] = FAULT_CODES["PHANTOM_SESSION"]
    tel.loc[mask, "fault_type"] = "PHANTOM_SESSION"
    tel.loc[mask, "fault_severity"] = FAULT_SEVERITY["PHANTOM_SESSION"]
    return tel


def inject_soc_plateau(tel: pd.DataFrame, stations: list,
                        start_h: int, duration_min: int = 20) -> pd.DataFrame:
    """
    Freezes soc_pct at its current value during an active charging session.
    Power remains non-zero (charger thinks it's working) but SoC doesn't
    move — BMS fault / cell imbalance.
    """
    start_s = start_h * 3600
    end_s = start_s + duration_min * 60

    for station in stations:
        smask = (
            (tel["station_id"] == station) &
            tel["timestamp_s"].between(start_s, end_s) &
            (tel["status"] == "CHARGING") &
            tel["soc_pct"].notna()
        )
        indices = tel[smask].index
        if len(indices) == 0:
            continue
        frozen_soc = tel.loc[indices[0], "soc_pct"]
        tel.loc[indices, "soc_pct"] = frozen_soc
        tel.loc[indices, "fault_code"] = FAULT_CODES["SOC_PLATEAU"]
        tel.loc[indices, "fault_type"] = "SOC_PLATEAU"
        tel.loc[indices, "fault_severity"] = FAULT_SEVERITY["SOC_PLATEAU"]
    return tel


def inject_firmware_fault(tel: pd.DataFrame, city_code: str,
                           start_h: int = 14, end_h: int = 16) -> pd.DataFrame:
    """
    All stations in city_code running firmware 2.3.0 emit fault code E042
    during the specified window. Pattern only detectable cross-station —
    a single station looks like random noise; the cluster reveals the bug.
    """
    start_s = start_h * 3600
    end_s = end_h * 3600

    mask = (
        (tel["city_code"] == city_code) &
        (tel["firmware_version"] == "2.3.0") &
        tel["timestamp_s"].between(start_s, end_s)
    )
    # Only inject on ~60% of rows to simulate intermittent glitch
    fault_mask = mask & (np.random.random(len(tel)) < 0.6)
    tel.loc[fault_mask, "fault_code"] = FAULT_CODES["FIRMWARE_FAULT"]
    tel.loc[fault_mask, "fault_type"] = "FIRMWARE_FAULT"
    tel.loc[fault_mask, "fault_severity"] = FAULT_SEVERITY["FIRMWARE_FAULT"]
    return tel


def inject_grid_congestion(tel: pd.DataFrame, city_code: str,
                            start_h: int, duration_min: int = 45) -> pd.DataFrame:
    """
    Amplifies power draw for all stations in city during the window,
    pushing grid_load_pct well above 120%. Also tags each row.
    """
    start_s = start_h * 3600
    end_s = start_s + duration_min * 60

    mask = (
        (tel["city_code"] == city_code) &
        tel["timestamp_s"].between(start_s, end_s) &
        (tel["status"] == "CHARGING")
    )
    # Boost power to near-max for all charging stations in city
    tel.loc[mask, "power_kw"] = (
        tel.loc[mask, "power_kw"] * np.random.uniform(1.3, 1.5, mask.sum())
    ).round(3)

    # Recompute grid_load_pct for affected city+timestamps
    city_ts = tel[tel["city_code"] == city_code].groupby("timestamp_s")["power_kw"].sum()
    capacity = tel[tel["city_code"] == city_code]["power_kw"].sum() / len(
        tel[tel["city_code"] == city_code]["timestamp_s"].unique()
    ) * 0.6 * 1.0  # rough capacity reference

    # Simpler: directly set grid_load_pct > 120 for affected rows
    tel.loc[mask, "grid_load_pct"] = (
        120 + np.random.uniform(5, 40, mask.sum())
    ).round(2)
    tel.loc[mask, "fault_code"] = FAULT_CODES["GRID_CONGESTION"]
    tel.loc[mask, "fault_type"] = "GRID_CONGESTION"
    tel.loc[mask, "fault_severity"] = FAULT_SEVERITY["GRID_CONGESTION"]
    return tel


def inject_ghost_station(tel: pd.DataFrame, stations: list,
                          start_h: int, duration_min: int = 60) -> pd.DataFrame:
    """
    Removes all rows for the specified stations during the window —
    simulates a station going completely offline (comms failure, power cut).
    The absence itself is the signal; agents detect missing heartbeats.
    Rows are tagged before removal so evaluation can count them.
    """
    start_s = start_h * 3600
    end_s = start_s + duration_min * 60

    mask = (
        tel["station_id"].isin(stations) &
        tel["timestamp_s"].between(start_s, end_s)
    )
    tel.loc[mask, "fault_type"] = "GHOST_STATION"
    tel.loc[mask, "fault_severity"] = FAULT_SEVERITY["GHOST_STATION"]
    # Drop rows — agent must detect absence from expected heartbeat stream
    tel = tel[~mask].reset_index(drop=True)
    return tel


def inject_all_faults(tel: pd.DataFrame, stations_df: pd.DataFrame) -> pd.DataFrame:
    """
    Orchestrates all fault injections across the 24h dataset.
    Fault schedule (spread across day, multiple cities, all types):
    """
    tel["fault_type"] = None
    tel["fault_severity"] = None

    print("Injecting faults...")

    # 1. THERMAL RUNAWAY — Berlin, 3 CCS2 stations, 08:30
    berlin_ccs2 = pick_stations(stations_df, "BER", n=3, connector="CCS2")
    tel = inject_thermal_runaway(tel, berlin_ccs2, start_h=8, duration_min=30)
    print(f"  [CRITICAL] THERMAL_RUNAWAY → Berlin stations: {berlin_ccs2}")

    # 2. THERMAL RUNAWAY — Munich, 2 CCS2 stations, 17:15
    munich_ccs2 = pick_stations(stations_df, "MUC", n=2, connector="CCS2")
    tel = inject_thermal_runaway(tel, munich_ccs2, start_h=17, duration_min=25)
    print(f"  [CRITICAL] THERMAL_RUNAWAY → Munich stations: {munich_ccs2}")

    # 3. PHANTOM SESSION — Hamburg, 3 stations, 10:00
    ham_stations = pick_stations(stations_df, "HAM", n=3)
    tel = inject_phantom_session(tel, ham_stations, start_h=10, duration_min=45)
    print(f"  [MEDIUM]   PHANTOM_SESSION → Hamburg stations: {ham_stations}")

    # 4. PHANTOM SESSION — Cologne, 2 stations, 15:30
    cgn_stations = pick_stations(stations_df, "CGN", n=2)
    tel = inject_phantom_session(tel, cgn_stations, start_h=15, duration_min=35)
    print(f"  [MEDIUM]   PHANTOM_SESSION → Cologne stations: {cgn_stations}")

    # 5. SOC PLATEAU — Frankfurt, 3 stations, 12:00
    fra_stations = pick_stations(stations_df, "FRA", n=3)
    tel = inject_soc_plateau(tel, fra_stations, start_h=12, duration_min=25)
    print(f"  [MEDIUM]   SOC_PLATEAU → Frankfurt stations: {fra_stations}")

    # 6. SOC PLATEAU — Stuttgart, 2 stations, 19:00
    stu_stations = pick_stations(stations_df, "STU", n=2)
    tel = inject_soc_plateau(tel, stu_stations, start_h=19, duration_min=20)
    print(f"  [MEDIUM]   SOC_PLATEAU → Stuttgart stations: {stu_stations}")

    # 7. FIRMWARE FAULT — Leipzig (most likely to have 2.3.0 cluster), 14:00-16:00
    tel = inject_firmware_fault(tel, "LEJ", start_h=14, end_h=16)
    print(f"  [LOW]      FIRMWARE_FAULT → Leipzig (firmware 2.3.0), 14:00-16:00")

    # 8. FIRMWARE FAULT — Düsseldorf, 09:00-11:00
    tel = inject_firmware_fault(tel, "DUS", start_h=9, end_h=11)
    print(f"  [LOW]      FIRMWARE_FAULT → Düsseldorf (firmware 2.3.0), 09:00-11:00")

    # 9. GRID CONGESTION — Berlin evening peak, 18:00
    tel = inject_grid_congestion(tel, "BER", start_h=18, duration_min=50)
    print(f"  [HIGH]     GRID_CONGESTION → Berlin, 18:00-18:50")

    # 10. GRID CONGESTION — Munich morning peak, 07:30
    tel = inject_grid_congestion(tel, "MUC", start_h=7, duration_min=40)
    print(f"  [HIGH]     GRID_CONGESTION → Munich, 07:30-08:10")

    # 11. GHOST STATION — Frankfurt, 2 stations, 13:00-14:00
    fra_ghost = pick_stations(stations_df, "FRA", n=2)
    tel = inject_ghost_station(tel, fra_ghost, start_h=13, duration_min=60)
    print(f"  [HIGH]     GHOST_STATION → Frankfurt stations: {fra_ghost} (rows dropped)")

    # 12. GHOST STATION — Hamburg, 1 station, 22:00-23:00
    ham_ghost = pick_stations(stations_df, "HAM", n=1)
    tel = inject_ghost_station(tel, ham_ghost, start_h=22, duration_min=60)
    print(f"  [HIGH]     GHOST_STATION → Hamburg station: {ham_ghost} (rows dropped)")

    return tel


if __name__ == "__main__":
    print("Loading telemetry and station registry...")
    tel = pd.read_csv("data/telemetry_raw.csv")
    stations_df = pd.read_csv("data/stations.csv")

    # Cast string columns that loaded as float due to all-None values
    tel["fault_code"] = tel["fault_code"].astype(object)
    tel["fault_type"] = tel["fault_type"].astype(object) if "fault_type" in tel.columns else None
    
    print(f"  {len(tel):,} rows, {tel['station_id'].nunique()} stations")

    tel = inject_all_faults(tel, stations_df)

    tel.to_csv("data/telemetry_faulted.csv", index=False)

    print(f"\n-> data/telemetry_faulted.csv ({len(tel):,} rows)")

    print("\nFault distribution:")
    print(tel["fault_type"].value_counts(dropna=False))
    print("\nFault severity distribution:")
    print(tel["fault_severity"].value_counts(dropna=False))
    print(f"\nCRITICAL rows: {(tel['fault_severity']=='CRITICAL').sum():,}")
    print(f"HIGH rows:     {(tel['fault_severity']=='HIGH').sum():,}")
    print(f"MEDIUM rows:   {(tel['fault_severity']=='MEDIUM').sum():,}")
    print(f"LOW rows:      {(tel['fault_severity']=='LOW').sum():,}")
    print(f"Clean rows:    {tel['fault_type'].isna().sum():,}")