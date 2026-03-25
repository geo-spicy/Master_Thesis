#!/bin/bash
#SBATCH --job-name=merra2_comp_download
#SBATCH --chdir=/work/aminr/
#SBATCH --output=/work/aminr/jobs/%x-%j.log
#SBATCH --time=2-12:00:00
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=8G

echo "Starting MERRA2 TSOIL2/3/4 download"
echo "Job ID: $SLURM_JOB_ID"
echo "Start time: $(date)"

# 1. Activate your correct venv
source /home/aminr/ms_eve/bin/activate

# 2. Quick sanity check
python - << 'EOF'
import sys, os
print("Python:", sys.version)
import requests
print("requests:", requests.__version__)
from netrc import netrc
path = os.path.expanduser("~/.netrc")
print(".netrc exists:", os.path.exists(path))
if os.path.exists(path):
    print("urs entry:", netrc(path).authenticators("urs.earthdata.nasa.gov"))
EOF

# 3. Create the Python download script in HOME
cat > /home/aminr/merra2_download.py << 'EOF'
import os
import time
from datetime import datetime, timedelta
import requests
from netrc import netrc

def get_session():
    netrc_path = os.path.expanduser("~/.netrc")
    auth = netrc(netrc_path).authenticators("urs.earthdata.nasa.gov")
    if auth is None:
        raise RuntimeError("No urs.earthdata.nasa.gov entry in ~/.netrc")
    username, _, password = auth

    s = requests.Session()
    s.auth = (username, password)
    s.headers.update({"User-Agent": "merra2-cluster-download/1.0"})
    return s

def download_merra2_tsoil234_subset():
    # Where the data files will be written:
    output_dir = "/work/aminr/MERRA2/Data/level2_4"
    os.makedirs(output_dir, exist_ok=True)

    session = get_session()
    base_otf_url = "https://goldsmr4.gesdisc.eosdis.nasa.gov/daac-bin/OTF/HTTP_services.cgi"

    fixed_params = {
        "SHORTNAME": "M2T1NXLND",
        "BBOX": "-90,-180,90,180",
        "SERVICE": "L34RS_MERRA2",
        "DATASET_VERSION": "5.12.4",
        "FORMAT": "bmM0Lw",
        "VERSION": "1.02",
        "VARIABLES": "TSOIL2,TSOIL3,TSOIL4",
    }

    # Small test window first
    start_date = datetime(2012, 1, 1)
    end_date   = datetime(2020, 12, 31)

    current = start_date
    downloaded = 0
    skipped = 0
    failed = 0

    print("Downloading MERRA-2 TSOIL2/3/4 via subsetter")
    print(f"Output directory: {output_dir}")

    while current <= end_date:
        y = current.year
        m = current.month
        d = current.day

        base_name = f"MERRA2_400.tavg1_2d_lnd_Nx.{y:04d}{m:02d}{d:02d}"
        orig_fname = base_name + ".nc4"
        sub_fname  = base_name + ".SUB.nc"
        out_path = os.path.join(output_dir, sub_fname)

        if os.path.exists(out_path):
            print(f"{sub_fname} exists, skipping.")
            skipped += 1
            current += timedelta(days=1)
            continue

        params = fixed_params.copy()
        params["FILENAME"] = f"/data/MERRA2/M2T1NXLND.5.12.4/{y:04d}/{m:02d}/{orig_fname}"
        params["LABEL"] = sub_fname

        print(f"{current:%Y-%m-%d} -> {sub_fname}")
        try:
            r = session.get(base_otf_url, params=params, timeout=600)
            ct = (r.headers.get("Content-Type") or "").lower()
            print(f"  Status: {r.status_code}, Content-Type: {ct}")

            # Consider any non-HTML 200 response as data (HDF5/NetCDF-4)
            is_html = "text/html" in ct
            data = r.content
            is_hdf = data.startswith(b"\x89HDF\r\n\x1a\n")  # magic for HDF5

            if r.status_code == 200 and (is_hdf or not is_html):
                with open(out_path, "wb") as f:
                    f.write(data)
                size_mb = os.path.getsize(out_path) / (1024 * 1024)
                print(f"  Saved {out_path} ({size_mb:.1f} MB)")
                downloaded += 1
            else:
                print(f"  Unexpected response, first bytes: {data[:200]!r}")
                failed += 1
        except Exception as e:
            print(f"  Error for {current:%Y-%m-%d}: {e}")
            failed += 1

        current += timedelta(days=1)
        time.sleep(0.5)

    print("\\nDownload complete")
    print(f"Downloaded: {downloaded}")
    print(f"Skipped:    {skipped}")
    print(f"Failed:     {failed}")

if __name__ == "__main__":
    download_merra2_tsoil234_subset()
EOF

echo "Running MERRA2 download script..."
python /home/aminr/merra2_download.py

echo "Job completed at: $(date)"
