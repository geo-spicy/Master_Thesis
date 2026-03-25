#!/bin/bash
#SBATCH --job-name=merra2_surface_combine
#SBATCH --chdir=/data/twd_data/amin_data/surface_layer/MERRA2
#SBATCH --output=/work/aminr/jobs/%x-%j.log
#SBATCH --time=5-00:00:00
#SBATCH --cpus-per-task=40
#SBATCH --mem-per-cpu=8G

echo "Starting MERRA2–in situ SURFACE-LAYER combine job"
echo "Job ID: $SLURM_JOB_ID"
echo "Start time: $(date)"

# Activate your virtual environment
source /home/aminr/ms_eve/bin/activate

# Run the Python script via here-doc
python << 'EOF'
import os
from pathlib import Path
import pandas as pd
import xarray as xr
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')


def find_nearest_merra2_grid(target_lat, target_lon, merra2_lats, merra2_lons):
    lat_idx = int(np.argmin(np.abs(merra2_lats - target_lat)))
    lon_idx = int(np.argmin(np.abs(merra2_lons - target_lon)))
    actual_lat = float(merra2_lats[lat_idx])
    actual_lon = float(merra2_lons[lon_idx])
    return lat_idx, lon_idx, actual_lat, actual_lon


def extract_merra2_for_station(merra2_dir, target_lat, target_lon, start_date, end_date):
    """
    Extract hourly MERRA2 surface-layer soil temperature (TSOIL1)
    for one station using half-hourly values around each hour.

    Returns:
        merra2_data: dict {datetime -> {'ts_merra2': float}}
        grid_coords: (lat_merra2, lon_merra2)
    """
    print(f"  Extracting MERRA2 data for lat={target_lat:.4f}, lon={target_lon:.4f}")
    print(f"  Date range: {start_date} to {end_date}")

    start_date = pd.to_datetime(start_date).normalize()
    end_date = pd.to_datetime(end_date).normalize()

    merra2_data = {}
    grid_coords = None

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        file_path = os.path.join(merra2_dir, f"MERRA2_400.tavg1_2d_lnd_Nx.{date_str}.SUB.nc")

        if not os.path.exists(file_path):
            print(f"    Missing MERRA2 file: {file_path}")
            current_date += timedelta(days=1)
            continue

        print(f"    Reading: {os.path.basename(file_path)}")

        try:
            with xr.open_dataset(file_path) as ds:
                lats = ds["lat"].values
                lons = ds["lon"].values

                # Determine nearest grid cell once, then reuse coordinates
                if grid_coords is None:
                    lat_idx, lon_idx, actual_lat, actual_lon = find_nearest_merra2_grid(
                        target_lat, target_lon, lats, lons
                    )
                    grid_coords = (actual_lat, actual_lon)
                    print(f"    MERRA2 grid: lat={actual_lat:.2f}, lon={actual_lon:.2f}")
                else:
                    lat_idx = int(np.argmin(np.abs(lats - grid_coords[0])))
                    lon_idx = int(np.argmin(np.abs(lons - grid_coords[1])))

                # Surface layer only: TSOIL1
                tsoil1 = ds["TSOIL1"].isel(lat=lat_idx, lon=lon_idx).values
                times = pd.to_datetime(ds["time"].values)

                # Collect valid half-hourly data
                half_hourly_data = {}
                for t, t1 in zip(times, tsoil1):
                    t = pd.Timestamp(t).floor("S")

                    def is_valid_temp(temp):
                        if pd.isna(temp) or temp == -9999 or temp <= 0:
                            return False
                        return 200 <= temp <= 350

                    if is_valid_temp(t1):
                        half_hourly_data[t] = {
                            "ts_merra2": float(t1)
                        }

                # Build hourly values from half-hourly values at ±30 min
                day_start = current_date
                for hour in range(24):
                    hourly_dt = day_start + timedelta(hours=hour)

                    prev_half = hourly_dt - timedelta(minutes=30)
                    next_half = hourly_dt + timedelta(minutes=30)

                    prev_data = half_hourly_data.get(prev_half)
                    next_data = half_hourly_data.get(next_half)

                    if prev_data and next_data:
                        merra2_data[hourly_dt] = {
                            "ts_merra2": (prev_data["ts_merra2"] + next_data["ts_merra2"]) / 2.0
                        }
                    elif prev_data:
                        merra2_data[hourly_dt] = prev_data.copy()
                    elif next_data:
                        merra2_data[hourly_dt] = next_data.copy()

        except Exception as e:
            print(f"    Error reading {file_path}: {e}")

        current_date += timedelta(days=1)

    print(f"  Collected {len(merra2_data)} hourly MERRA2 TSOIL1 records")
    return merra2_data, grid_coords


def process_station_merra2_insitu(station_csv_path, merra2_dir, output_dir):
    """
    For one in-situ station CSV:
      - infer datetime range
      - extract TSOIL1 from MERRA2 at nearest grid cell
      - build hourly ts_merra2 (K)
      - merge with in-situ ts_station_k
      - enforce strict pairing between ts_station_k and ts_merra2
      - drop T_* and old ts_station and write combined CSV
    """
    print(f"\n🌡️ Processing station file: {station_csv_path}")

    try:
        df = pd.read_csv(station_csv_path)
        print(f"  In-situ records: {len(df)}")
    except Exception as e:
        print(f"  ❌ Error reading CSV: {e}")
        return False

    # Ensure datetime column
    if "datetime" not in df.columns and "date" in df.columns and "time" in df.columns:
        df["datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))

    required_cols = ["lat", "lon", "datetime"]
    if not all(col in df.columns for col in required_cols):
        print(f"  ❌ Missing required columns: {required_cols}")
        return False

    station_lat = df["lat"].iloc[0]
    station_lon = df["lon"].iloc[0]

    df["datetime"] = pd.to_datetime(df["datetime"])
    start_date = df["datetime"].min().date()
    end_date = df["datetime"].max().date()

    print(f"  Station: lat={station_lat:.4f}, lon={station_lon:.4f}")
    print(f"  Date range: {start_date} to {end_date}")

    # Extract MERRA2 surface-layer (TSOIL1) for this station
    merra2_data, grid_coords = extract_merra2_for_station(
        merra2_dir, station_lat, station_lon, start_date, end_date
    )

    if not merra2_data:
        print(f"  ❌ No MERRA2 data found")
        return False

    # Build MERRA2 dataframe
    merra2_records = []
    for dt, temps in merra2_data.items():
        merra2_records.append({
            "datetime": dt,
            "ts_merra2": round(temps["ts_merra2"], 3),
            "lat_merra2": grid_coords[0],
            "lon_merra2": grid_coords[1],
        })

    merra2_df = pd.DataFrame(merra2_records)
    merra2_df["datetime"] = pd.to_datetime(merra2_df["datetime"])
    print(f"  MERRA2 records: {len(merra2_df)}")

    # Merge in-situ with MERRA2
    df["datetime"] = pd.to_datetime(df["datetime"])
    merged_df = df.merge(merra2_df, on="datetime", how="left")

    print(f"  Merged records: {len(merged_df)}")
    print(f"  MERRA2 matches: {merged_df['ts_merra2'].notna().sum()}")

    # Drop any old ts_station and all T_* columns from in-situ
    if "ts_station" in merged_df.columns:
        merged_df = merged_df.drop(columns=["ts_station"])

    t_columns = [col for col in merged_df.columns if col.startswith("T_")]
    if t_columns:
        merged_df = merged_df.drop(columns=t_columns)
        print(f"  Dropped T_ columns: {t_columns}")

    # Ensure ts_merra2 sits after ts_station_k, if both exist
    columns = merged_df.columns.tolist()
    if "ts_merra2" in columns and "ts_station_k" in columns:
        columns.remove("ts_merra2")
        idx = columns.index("ts_station_k")
        columns.insert(idx + 1, "ts_merra2")
        merged_df = merged_df[columns]

    # Strict paired availability: if one side is missing, set the other to NaN
    if "ts_station_k" in merged_df.columns:
        merra2_missing = merged_df["ts_merra2"].isna()
        if merra2_missing.any():
            merged_df.loc[merra2_missing, "ts_station_k"] = pd.NA
            print(f"  Set {merra2_missing.sum()} station values to NaN where MERRA2 missing")

        station_missing = merged_df["ts_station_k"].isna()
        if station_missing.any():
            merged_df.loc[station_missing, "ts_merra2"] = pd.NA
            print(f"  Set {station_missing.sum()} MERRA2 values to NaN where station missing")

    # Write output
    os.makedirs(output_dir, exist_ok=True)

    input_filename = os.path.basename(station_csv_path)
    output_filename = input_filename.replace(".csv", "_merra2.csv")
    output_path = os.path.join(output_dir, output_filename)

    merged_df.to_csv(output_path, index=False)
    print(f"  ✅ Saved: {output_path}")

    # Optional sample print
    sample_cols = ["datetime", "ts_merra2"]
    sample = merged_df[sample_cols].dropna().head(3)
    if not sample.empty:
        print("  📊 Sample results:")
        for _, row in sample.iterrows():
            print(f"    {row['datetime']}: ts_merra2={row['ts_merra2']:.3f} K")

    return True


def main():
    # Surface-layer roots
    IN_ROOT = Path("/data/twd_data/amin_data/surface_layer/in_situ/combine")
    MERRA2_DIR = "/work/aminr/MERRA2/Data/level_1"
    OUT_ROOT = Path("/data/twd_data/amin_data/surface_layer/MERRA2/combine")

    print("🚀 MERRA2 + In-situ Soil Temperature Combination (SURFACE LAYER, TSOIL1)")
    print("=" * 80)
    print(f"In-situ root: {IN_ROOT}")
    print(f"MERRA2 dir  : {MERRA2_DIR}")
    print(f"Output root : {OUT_ROOT}")

    total_networks = 0
    total_stations = 0
    processed_stations = 0
    error_stations = 0

    # Loop over all networks
    for network_dir in sorted(IN_ROOT.iterdir()):
        if not network_dir.is_dir():
            continue
        network_name = network_dir.name
        total_networks += 1
        print(f"\n{'-'*80}")
        print(f"🌐 Network: {network_name}")
        print(f"{'-'*80}")

        # Loop over all stations in this network
        for station_dir in sorted(network_dir.iterdir()):
            if not station_dir.is_dir():
                continue
            station_name = station_dir.name
            total_stations += 1

            csv_files = list(station_dir.glob("*.csv"))
            if not csv_files:
                print(f"  ⚠️ No CSV found for station {station_name} in {station_dir}")
                error_stations += 1
                continue

            station_csv = csv_files[0]

            print(f"\n➡️ Station: {network_name}/{station_name}")
            print(f"  CSV: {station_csv}")

            station_out_dir = OUT_ROOT / network_name / station_name
            os.makedirs(station_out_dir, exist_ok=True)

            # Restart-safe: if output already exists, skip this station
            input_filename = os.path.basename(station_csv)
            output_filename = input_filename.replace(".csv", "_merra2.csv")
            output_path = station_out_dir / output_filename

            if output_path.exists():
                print(f"  ⏩ Output already exists, skipping: {output_path}")
                continue

            try:
                ok = process_station_merra2_insitu(
                    station_csv_path=str(station_csv),
                    merra2_dir=MERRA2_DIR,
                    output_dir=str(station_out_dir),
                )
                if ok:
                    processed_stations += 1
                else:
                    error_stations += 1
            except Exception as e:
                print(f"  ❌ Unexpected error for {network_name}/{station_name}: {e}")
                error_stations += 1

    print(f"\n{'='*80}")
    print("PROCESSING SUMMARY:")
    print(f"{'='*80}")
    print(f"Total networks processed: {total_networks}")
    print(f"Total stations found    : {total_stations}")
    print(f"Successfully processed  : {processed_stations}")
    print(f"Errors/skipped          : {error_stations}")
    if total_stations > 0:
        print(f"Success rate            : {processed_stations/total_stations*100:.1f}%")
    else:
        print("Success rate            : 0%")
    print(f"Output directory        : {OUT_ROOT}")
    print("Done.")


if __name__ == "__main__":
    main()
EOF

echo "Job completed at: $(date)"
