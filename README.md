# Soil Temperature Analysis Scripts

This repository contains the scripts used in my Master's thesis for analyzing soil temperature (ST) from multiple products at surface and root-zone layers using in-situ networks from the 
International Soil Moisture Network (ISMN). **Data is not included**, only the code.

---

## Folder Structure

root/
├── surface_layer/
│ ├── era5/
│ ├── gldas/
│ ├── merra/
│ └── lprm/
├── rootzone_layer/
│ ├── era5/
│ ├── gldas/
│ ├── merra/
└── Excel_tables/

# Surface foler, rootzone_layer
This folder contains multiple subfolders for each product that were used at the surface and root-zone layers. Each subfolder contains data downloading, and exploration, combining, and pixel mean scripts and their analysis.

# Excel_tables
This folder contains all error metrics for each ST product for each in-situ ST network at the surface and root-zone layers, both at native resolution and temporal harmonized (4_timestamps)
