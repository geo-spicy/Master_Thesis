#!/bin/bash
#SBATCH --job-name=gldas_comp_download
#SBATCH --chdir=/work/aminr/
#SBATCH --output=/work/aminr/jobs/%x-%j.log
#SBATCH --time=2-23:00:00
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=8G

echo "Starting GLDAS SoilTMP10_40cm_inst & SoilTMP40_100cm_inst download - GLOBAL SCALE"
echo "Job ID: $SLURM_JOB_ID"
echo "Start time: $(date)"

# Activate your Python environment
source /home/aminr/ms_eve/bin/activate

# Quick check
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

# Create the Python download script
cat > /home/aminr/gldas_download.py << 'EOF'
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
    s.headers.update({"User-Agent": "gldas-cluster-download/1.0"})
    return s

def download_gldas_global():
    """
    Download GLDAS_NOAH025_3H GLDAS-2.1 for:
      - SoilTMP10_40cm_inst
      - SoilTMP40_100cm_inst
    globally using GES DISC OTF subsetter.
    """

    output_dir = "/work/aminr/GLDAS_Noah/data/level_2"
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")

    session = get_session()

    base_otf_url = "https://hydro1.gesdisc.eosdis.nasa.gov/daac-bin/OTF/HTTP_services.cgi"

    base_params = {
        "FORMAT": "bmM0Lw",
        "VARIABLES": "SoilTMP10_40cm_inst,SoilTMP40_100cm_inst",
        "SERVICE": "L34RS_LDAS",
        "BBOX": "-60.0,-180.0,90.0,180.0",
        "VERSION": "1.02",
        "DATASET_VERSION": "2.1",
        "SHORTNAME": "GLDAS_NOAH025_3H",
    }

    # Start with the same dates as your local test (you can extend later)
    start_date = datetime(2012, 1, 1)
    end_date   = datetime(2020, 12, 31)
    times = ["0000", "0300", "0600", "0900", "1200", "1500", "1800", "2100"]

    downloaded = 0
    skipped = 0
    failed = 0

    print("Starting GLDAS download - GLOBAL SCALE")
    print("Variables: SoilTMP10_40cm_inst, SoilTMP40_100cm_inst")
    print("Coverage: lat -60 to 90, lon -180 to 180")
    print(f"Date range: {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}")

    current_date = start_date
    while current_date <= end_date:
        year = current_date.strftime("%Y")
        doy = current_date.strftime("%j")
        date_str = current_date.strftime("%Y%m%d")

        print(f"Processing {current_date:%Y-%m-%d}...")

        for hour in times:
            orig_fname = f"GLDAS_NOAH025_3H.A{date_str}.{hour}.021.nc4"
            out_fname  = f"GLDAS_NOAH025_3H.A{date_str}.{hour}.021.nc4.SUB.nc4"
            out_path   = os.path.join(output_dir, out_fname)

            if os.path.exists(out_path):
                skipped += 1
                continue

            file_path = f"/data/GLDAS/GLDAS_NOAH025_3H.2.1/{year}/{doy}/{orig_fname}"
            params = base_params.copy()
            params["FILENAME"] = file_path
            params["LABEL"] = out_fname

            try:
                r = session.get(base_otf_url, params=params, timeout=600)
                ct = (r.headers.get("Content-Type") or "").lower()
                print(f"  {out_fname} -> Status: {r.status_code}, Content-Type: {ct}")

                is_html = "text/html" in ct
                data = r.content

                # Accept any non-HTML 200 response as data (HDF5/NetCDF-4)
                if r.status_code == 200 and not is_html:
                    with open(out_path, "wb") as f:
                        f.write(data)
                    size_mb = os.path.getsize(out_path) / (1024 * 1024)
                    print(f"    Saved {out_path} ({size_mb:.2f} MB)")
                    downloaded += 1
                else:
                    print(f"    Unexpected response, first bytes: {data[:200]!r}")
                    failed += 1

            except Exception as e:
                print(f"    Error for {out_fname}: {e}")
                failed += 1

            time.sleep(0.5)

        current_date += timedelta(days=1)

    print("\\nDownload complete")
    print(f"Downloaded: {downloaded}")
    print(f"Skipped (existing): {skipped}")
    print(f"Failed: {failed}")
    print(f"Total processed: {downloaded + skipped + failed}")

if __name__ == "__main__":
    download_gldas_global()
EOF

echo "Running GLDAS download script..."
python /home/aminr/gldas_download.py

echo "Job completed at: $(date)"

# Optional cleanup
# rm -f /home/aminr/gldas_download.py
