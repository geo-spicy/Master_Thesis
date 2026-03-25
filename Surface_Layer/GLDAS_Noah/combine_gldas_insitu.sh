#!/bin/bash
#SBATCH --job-name=gldas_resume_surface_combine
#SBATCH --chdir=/data/twd_data/amin_data/surface_layer/GLDAS_Noah
#SBATCH --output=/work/aminr/jobs/%x-%j.log
#SBATCH --time=7-00:00:00
#SBATCH --cpus-per-task=40
#SBATCH --mem-per-cpu=8G

echo "Starting GLDAS–in situ SURFACE-LAYER combine job"
echo "Job ID: $SLURM_JOB_ID"
echo "Start time: $(date)"

source /home/aminr/ms_eve/bin/activate

python << 'EOF'
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr

# ------------------------------------------------------------------
# Paths (surface layer)
# ------------------------------------------------------------------
INSITU_ROOT = Path("/data/twd_data/amin_data/surface_layer/in_situ/combine")
GLDAS_ROOT  = Path("/work/aminr/GLDAS_Noah/data/level_1")
OUT_ROOT    = Path("/data/twd_data/amin_data/surface_layer/GLDAS_Noah/combine")
OUT_ROOT.mkdir(parents=True, exist_ok=True)

# GLDAS is 3-hourly; keep only these hours
three_hour_hours = {"00","03","06","09","12","15","18","21"}

# ------------------------------------------------------------------
# Helper: load GLDAS (SoilTMP0_10cm_inst) at nearest grid point for given years
# ------------------------------------------------------------------
def load_gldas_for_years(years, lat_station, lon_station):
    """
    For a station at (lat_station, lon_station), load all GLDAS Noah level-1
    files for the given years, extract SoilTMP0_10cm_inst at the nearest grid
    cell, and return a DataFrame:

        datetime, ts_gldas, lat_gldas, lon_gldas
    """
    dfs = []
    lat_g = None
    lon_g = None

    for year in sorted(set(years)):
        pattern = f"GLDAS_NOAH025_3H.A{year}*.021.nc4.SUB.nc4"
        files = sorted(GLDAS_ROOT.glob(pattern))
        if not files:
            print(f"  No GLDAS files for year {year}")
            continue

        for f in files:
            try:
                ds = xr.open_dataset(f)
            except Exception as e:
                print(f"    Failed to open {f}: {e}")
                continue

            # GLDAS uses lon in -180..180, so no conversion needed
            ds_sel = ds.sel(
                lat=lat_station,
                lon=lon_station,
                method="nearest"
            )

            lat_val = float(ds_sel["lat"].values)
            lon_val = float(ds_sel["lon"].values)

            if lat_g is None:
                lat_g = lat_val
            if lon_g is None:
                lon_g = lon_val

            # one time per file
            t = pd.Timestamp(ds_sel["time"].values[0])

            v = float(ds_sel["SoilTMP0_10cm_inst"].values.squeeze())
            ds.close()

            if v == -9999.0:
                v = np.nan

            dfs.append(
                pd.DataFrame(
                    {
                        "datetime": [t],
                        "ts_gldas": [v],
                    }
                )
            )

    if not dfs:
        return None

    gldas_df = pd.concat(dfs, ignore_index=True).sort_values("datetime")
    gldas_df["lat_gldas"] = lat_g
    gldas_df["lon_gldas"] = lon_g
    gldas_df["datetime"] = pd.to_datetime(gldas_df["datetime"])

    return gldas_df

# ------------------------------------------------------------------
# Process ALL networks/stations (surface layer)
# ------------------------------------------------------------------
total_networks = 0
total_stations = 0
processed_stations = 0
error_stations = 0

for network_dir in sorted(INSITU_ROOT.iterdir()):
    if not network_dir.is_dir():
        continue

    network_name = network_dir.name
    total_networks += 1

    print(f"\n=== Processing Network: {network_name} ===")

    out_network_dir = OUT_ROOT / network_name
    out_network_dir.mkdir(parents=True, exist_ok=True)

    for station_dir in sorted(network_dir.iterdir()):
        if not station_dir.is_dir():
            continue

        st_id = station_dir.name
        total_stations += 1

        # ------------------------------------------------------
        # RESUME LOGIC: skip station if output already exists
        # ------------------------------------------------------
        out_station_dir = out_network_dir / st_id
        out_csv = out_station_dir / f"{network_name}_{st_id}_insitu_gldas_surface_soil_temperature.csv"
        if out_csv.exists():
            print(f"  {st_id}: output already exists, skipping.")
            processed_stations += 1
            continue
        # ------------------------------------------------------

        station_files = list(station_dir.glob("*_soil_temperature_depths.csv"))

        if not station_files:
            print(f"  {st_id}: no in-situ CSV, skipping.")
            error_stations += 1
            continue

        csv_path = station_files[0]
        print(f"  Processing station {st_id}: {csv_path.name}")

        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            print(f"    Failed to read {csv_path}: {e}")
            error_stations += 1
            continue

        needed = [
            "date","time",
            "ts_station_k",
            "lat","lon",
            "elev","cc","lc",
            "land_cover_group","climate_group",
            "temp_class","elevation_class",
        ]
        missing = [c for c in needed if c not in df.columns]
        if missing:
            print(f"    Missing columns {missing}, skipping.")
            error_stations += 1
            continue

        # keep only 3-hourly times
        df = df[df["time"].astype(str).str.slice(0,2).isin(three_hour_hours)]
        if df.empty:
            print("    No 3-hourly timestamps, skipping.")
            error_stations += 1
            continue

        # build datetime and ensure proper type
        df["datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str), errors="coerce")
        df = df.dropna(subset=["datetime"]).sort_values("datetime")
        if df.empty:
            print("    No valid datetimes, skipping.")
            error_stations += 1
            continue

        lat_station = float(df["lat"].iloc[0])
        lon_station = float(df["lon"].iloc[0])
        years = df["datetime"].dt.year.unique()

        gldas_df = load_gldas_for_years(years, lat_station, lon_station)
        if gldas_df is None:
            print("    No GLDAS data found for this station, skipping.")
            error_stations += 1
            continue

        # Ensure both DataFrames have same datetime type before merge
        df["datetime"] = pd.to_datetime(df["datetime"])
        gldas_df["datetime"] = pd.to_datetime(gldas_df["datetime"])

        combined = pd.merge(df, gldas_df, on="datetime", how="inner")
        if combined.empty:
            print("    No overlapping timestamps, skipping.")
            error_stations += 1
            continue

        # Ensure ts_gldas is numeric and rounded
        combined["ts_gldas"] = pd.to_numeric(combined["ts_gldas"], errors="coerce").round(3)

        # Strict paired availability: if one side is missing, set the other to NaN
        if "ts_station_k" in combined.columns:
            gldas_missing = combined["ts_gldas"].isna()
            if gldas_missing.any():
                combined.loc[gldas_missing, "ts_station_k"] = np.nan
                print(f"    Set {gldas_missing.sum()} station values to NaN where GLDAS missing")

            station_missing = combined["ts_station_k"].isna()
            if station_missing.any():
                combined.loc[station_missing, "ts_gldas"] = np.nan
                print(f"    Set {station_missing.sum()} GLDAS values to NaN where station missing")

        # insert ts_gldas after ts_station_k (if needed)
        if "ts_gldas" in combined.columns and "ts_station_k" in combined.columns:
            cols = list(combined.columns)
            cols.remove("ts_gldas")
            idx = cols.index("ts_station_k")
            cols.insert(idx + 1, "ts_gldas")
            combined = combined[cols]

        # convert datetime back to date / time columns
        combined["date"] = combined["datetime"].dt.strftime("%Y-%m-%d")
        combined["time"] = combined["datetime"].dt.strftime("%H:%M")
        combined = combined.drop(columns=["datetime"])

        # drop original in-situ depth profile columns
        drop_cols = [c for c in combined.columns if c.startswith("T_")]
        if "ts_station" in combined.columns:
            drop_cols.append("ts_station")
        if drop_cols:
            combined = combined.drop(columns=drop_cols)

        # final column order (no ts_station)
        meta_cols = [
            "date","time",
            "ts_station_k","ts_gldas",
            "lat","lon","lat_gldas","lon_gldas",
            "elev","cc","lc",
            "land_cover_group","climate_group",
            "temp_class","elevation_class",
        ]
        other_cols = [c for c in combined.columns if c not in meta_cols]

        final_cols = [c for c in meta_cols + other_cols if c in combined.columns]
        combined = combined[final_cols]

        # Save to network-specific output directory
        out_station_dir.mkdir(parents=True, exist_ok=True)
        combined.to_csv(out_csv, index=False, float_format="%.3f")

        processed_stations += 1
        print(f"    ✓ Wrote: {out_csv}")

print(f"\n{'='*80}")
print("PROCESSING SUMMARY:")
print(f"{'='*80}")
print(f"Total networks processed: {total_networks}")
print(f"Total stations found: {total_stations}")
print(f"Successfully processed (incl. already-existing): {processed_stations}")
print(f"Errors/skipped (no data, missing cols, etc.): {error_stations}")
if total_stations > 0:
    print(f"Success rate: {processed_stations/total_stations*100:.1f}%")
else:
    print("Success rate: 0%")
print(f"Output directory: {OUT_ROOT}")
print("Done.")
EOF

echo "Job completed at: $(date)"
