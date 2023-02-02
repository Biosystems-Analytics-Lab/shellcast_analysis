# ShellCast PQPF

### Data
Data Name: Probabilistic Quantitative Precipitation Forecast (PQPF)
Source: NOAA 
Description of Products: https://www.nws.noaa.gov/mdl/hrqpf/pqpfdescription.php 
About Data: https://www.wpc.ncep.noaa.gov/pqpf/PDD_WPC_PQPF.pdf 
Download: https://ftp.wpc.ncep.noaa.gov/pqpf/conus/ 
Map: https://www.wpc.ncep.noaa.gov/pqpf/conus_hpc_pqpf.php?fpd=24 

### Input Data Preparation

This is most likely manually need to be process.

* Lease data having lease id, Conditional Management Unit (CMU) name, and rainfall thresholds (inch).
* Make sure no duplicates in lease id.
* Check geometry

### Configuration

Two files ```config.ini``` and  ```main.py``` to configure.

#### config.ini

Use **config.ini** file to configure analysis. Brackets are the sections.  For example, the section **[NC]** has seven variables. Change these when data changed. The coordinates *LON_WE* and *LAT_SN* are used in order to crop GRB file
in small area (see https://www.cpc.ncep.noaa.gov/products/wesley/wgrib2/small_grib.html). 

| Variables              | Descriptions                                   | Example      |
| ---------------------- | ---------------------------------------------- | ------------ |
| DB_NAME                | Database name                                  | shellcast_nc |
| LEASE_SHP              | Lease shapefile name                           | leases.shp   |
| LEASE_SHP_COL_LEASE_ID | Column name for lease id                       | lease_id     |
| LEASE_SHP_COL_CMU_NAME | Column name for CMU name                       | cmu_name     |
| LEASE_SHP_COL_RAIN_IN  | Column name for rainfall thresholds (inches)   | rain_in      |
| LON_WE                 | West longitude:Ease longitude to crop GRB file | 10:20        |
| LAT_SN                 | South latitude:North latitude to crop GRB file | -20:20       |



#### main.py

