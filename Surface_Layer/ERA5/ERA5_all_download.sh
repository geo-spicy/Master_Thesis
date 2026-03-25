#!/bin/bash
#SBATCH --job-name=era5_L1_download
#SBATCH --chdir=/work/aminr/
#SBATCH --output=/work/aminr/jobs/%x-%j.log
#SBATCH --time=2-23:00:00          # 2 days
#SBATCH --cpus-per-task=32          # Single CPU
#SBATCH --mem-per-cpu=8G                   # 4GB memory

echo "Starting ERA5 soil temperature level 3 downloads for 2012-2020"
echo "Job ID: $SLURM_JOB_ID"
echo "Start time: $(date)"

# Load required modules
#module load python/3.9


#Activate your virtual environment
source /home/aminr/ms_eve/bin/activate

# Create the Python script (based on your original)
cat > /work/aminr/era5_download.py << 'EOF'
import cdsapi
import os

# Initialize CDS API client
client = cdsapi.Client()

# Base output directory  
base_output_dir = "/work/aminr/ERA5/Data/level_1"

# Years to download (2012-2020)
years = ["2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020"]

print("Starting ERA5 soil temperature level 1 downloads...")

for year in years:
    print(f"\n=== Downloading year: {year} ===")
    
    # Create year directory
    year_dir = os.path.join(base_output_dir, year)
    os.makedirs(year_dir, exist_ok=True)
    
    # Set output filename
    output_file = os.path.join(year_dir, f"era5_soil_temperature_global_{year}.zip")
    
    # Skip if already exists
    if os.path.exists(output_file):
        print(f"File already exists, skipping: {output_file}")
        continue
    
    # Same request as your original script, just with different year
    dataset = "reanalysis-era5-single-levels"
    request = {
        "product_type": ["reanalysis"],
        "year": [year],
        "month": [
            "01", "02", "03", "04", "05", "06",
            "07", "08", "09", "10", "11", "12"
        ],
        "day": [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
        ],
        "time": [
            "00:00", "01:00", "02:00", "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00", "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00", "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00", "21:00", "22:00", "23:00"
        ],
        "data_format": "netcdf",
        "download_format": "zip",
        "variable": ["soil_temperature_level_1"]
    }
    
    print(f"Requesting ERA5 soil temperature data for {year}...")
    print(f"Output path: {output_file}")
    print("This may take several hours...")
    
    try:
        result = client.retrieve(dataset, request)
        result.download(output_file)
        print(f"Download completed: {output_file}")
    except Exception as e:
        print(f"Download failed for {year}: {e}")
        print("Continuing to next year...")

print("All downloads finished!")
EOF

echo "Running ERA5 download script..."
python /work/aminr/era5_download.py

echo "Job completed at: $(date)"

# Clean up
rm -f /work/aminr/era5_download.py
