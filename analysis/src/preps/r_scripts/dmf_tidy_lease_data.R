# ---- script header ----
# script name: ncdmf_tidy_lease_data_script.R
# purpose of script: tidy up lease data from nc dmf rest api (assumes data is in .shp format)
# author: sheila saia
# date created: 20200617
# email: ssaia@ncsu.edu


# ---- notes ----
# notes:

# raw data column metadata
# ProductNbr - lease id
# Assoc_ID - another id associated with the lease id (we don't need to worry about this)
# Owner - owner/business name
# Bus_Agent - business agent
# County - NC county that the business is in
# WB_Name - waterbody name (this is not the same as growing area)
# Type_ - lease type (bottom - rent rights to bottom, water column, franchise - own rights to bottom, research sanctuary, proposed, terminated)
# Status - status of the lease (there are lots of different unique values here)
# A_Granted - acres granted in the lease
# EffectiveD - date approved/renewed
# Expiration - expiration date of lease, 9996 and 9994 indicate a franchise - these have no expiration dates and if part or all is not in a cmu then can't harvest from it
# Term_Date - termination date (i.e., date when the leased area was terminated)


# ----- Seed data -----
# /spatial/outputs/dmf_data/lease_bounds_raw/lease_bounds_raw.shp



dmf_tidy_lease_data <- function(lease_data_spatial_input_path, cmu_spatial_data_input_path, sga_spatial_data_input_path, rainfall_thresh_tabular_data_input_path, lease_data_spatial_output_path, lease_bounds_raw_shp) {

    # ---- 1. install and load packages as necessary ----
    # packages
    packages <- c("tidyverse", "sf", "geojsonsf")

    # install and load
    for (package in packages) {
      if (! package %in% installed.packages()) {
        install.packages(package, dependencies = TRUE)
      }
      library(package, character.only = TRUE)
    }

    library("tidyverse")
    library("sf")
    library("geojsonsf")

    # ---- 2. define base paths ----
    # base path to data
    # data_base_path = "opt/analysis/data/" # set this and uncomment!
    # data_base_path = "/Users/sheila/Documents/bae_shellcast_project/shellcast_analysis/web_app_data/"
    # data_base_path = "/Users/sheila/Documents/github_ncsu/shellcast/analysis/data/"
    # data_base_path = here::here("data")
#     data_base_path = '/analysis/data'
    # ---- 3. defining paths and projections ----
    # inputs
    # path to raw lease spatial inputs
#     lease_data_spatial_input_path <- paste0(data_base_path, "/spatial/outputs/ncdmf_data/lease_bounds_raw/")

    # path to cmu bounds spatial inputs
#     cmu_spatial_data_input_path <- paste0(data_base_path, "/spatial/inputs/ncdmf_data/cmu_bounds/")

    # path to sga bounds spatial inputs
#     sga_spatial_data_input_path <- paste0(data_base_path, "/spatial/inputs/ncdmf_data/sga_bounds/")

    # path to rainfall threshold tabular inputs
#     rainfall_thresh_tabular_data_input_path <- paste0(data_base_path, "/tabular/inputs/ncdmf_rainfall_thresholds/")

    # outputs
    # path to lease spatial outputs
#     lease_data_spatial_output_path <-  paste0(data_base_path, "/spatial/outputs/dmf_data/")

    # projections
    # define epsg and proj4 for N. America Albers projection (projecting to this)
    # na_albers_proj4 <- "+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"
    # na_albers_epsg <- 102008

    # define epsg and proj for CONUS Albers projection (projecting to this)
    conus_albers_epsg <- 5070
    conus_albers_proj <- "+init=EPSG:5070"

    # define wgs 84 projection
    wgs84_epsg <- 4326
    wgs84_proj4 <- "+proj=longlat +datum=WGS84 +no_defs"


    # ---- 4. load in lease and rainfall threshold data (without dates) ----
    # spatial data
    # use latest date to read in most recent data
    lease_bounds_raw <- st_read(paste0(lease_data_spatial_input_path, lease_bounds_raw_shp))

    # check projection
    # st_crs(lease_bounds_raw) # epsg = 2264

    # cmu data
    cmu_bounds_albers <- st_read(paste0(cmu_spatial_data_input_path, "cmu_bounds_albers.shp"))

    # sga data
    sga_bounds_simple_albers <- st_read(paste0(sga_spatial_data_input_path, "sga_bounds_simple_albers.shp"))

    # check projection
    # st_crs(cmu_bounds_albers) # epsg = 5070
    # st_crs(sga_bounds_albers) # epsg = 5070

    # tabular data
    # rainfall thresholds
    rainfall_thresholds_raw_tidy <- read_csv(paste0(rainfall_thresh_tabular_data_input_path, "rainfall_thresholds_raw_tidy.csv"))

    # sga key
    sga_key <- read_csv(paste0(rainfall_thresh_tabular_data_input_path, "sga_key.csv"))


    # ---- 5. project and tidy up lease data ----
    lease_data_albers <- lease_bounds_raw %>%
      st_transform(crs = conus_albers_epsg) %>% # project
      dplyr::select(lease_id = ProductNbr, # tidy
                    owner = Owner,
                    type = Type_,
                    area_ac = A_Granted,
                    status = Status,
                    water_body = WB_Name,
                    county = County)

    # check projection
    # st_crs(lease_data_albers)
    # crs = 5070


    # ---- 6. find centroids of leases ----
    # calculate centroids of leases for map pins
    lease_data_centroid_albers <- lease_data_albers %>%
      st_centroid()


    # ---- 7. add a little buffer to the sga bounds and select sga key data ----
    # sga key selected
    sga_key_sel <- sga_key %>%
      dplyr::select(grow_area = sga_name, sga_desc = sga_desc_short) %>%
      dplyr::mutate(word_count = str_count(sga_desc)) # has to be < 50 for the database

    # buffer sga bounds and bind sga key select
    sga_bounds_simple_50mbuf_albers <- sga_bounds_simple_albers %>%
      st_buffer(dist = 50) %>% # in m
      dplyr::left_join(sga_key_sel, by = "grow_area")


    # ---- 8. add sga and rainfall depths to lease data ----
    # bind sga's to cmu's
    # cmu_sga_bounds_join <- cmu_bounds_albers %>%
    #   st_intersection(sga_bounds_albers)
    #   dplyr::select(cmu_name, rain_in, rain_lab)

    # trim rainfall threshold data
    # rainfall_thresh_data_sel <- rainfall_thresholds_raw_tidy %>%
    #  dplyr::select(HA_CLASS, cmu_name)

    # join lease, cmu, and sga data by geometry
    # joining by cmu and not sga because each sga has mutiple cmu's within and therefore multiple rainfall thresholds
    lease_data_centroid_albers_join <- lease_data_centroid_albers %>%
      st_intersection(cmu_bounds_albers) %>%
      st_intersection(sga_bounds_simple_50mbuf_albers) %>%
      # dplyr::left_join(rainfall_thresh_data_sel, by = "HA_CLASS") %>%
      dplyr::group_by(lease_id) %>%
      dplyr::slice(which.min(rain_in)) %>%
      # dplyr::group_by(lease_id, rain_in) %>%
      # dplyr::count() %>%
      dplyr::select(lease_id:county, cmu_name, sga_name = grow_area, sga_desc, rain_in, rain_lab)

    # NOTE!
    # if the centroid of the lease is at the edge of two growing areas then this code takes
    # the minimum rainfall threshold, as of 20200716 it does not look like this is an issue
    # but it cold be in the future
    # there are ~ 20 (513-483 = 30) leases that have centroids not in cmu's, these are dropped
    # also, there were some leases that were just on the edge of the sga boundaries
    # with the max one about 37m away, so used a 50m buffer around sga bounds to help with this
    # might need an automated check for this once the REST API is up and running

    # keep sga and rainfall threshold data to join to lease polygons
    lease_data_centroids_join_no_geom <- lease_data_centroid_albers_join %>%
      st_drop_geometry() %>%
      dplyr::select(lease_id, cmu_name:rain_lab)

    # join sga and rainfall threshold data to lease polygons
    lease_data_albers_join <- lease_data_albers %>%
      dplyr::left_join(lease_data_centroids_join_no_geom, by = "lease_id") %>%
      dplyr::filter(rain_in > 0) # drop leases without rainfall thresholds (i.e., NA values) so don't have to fix for db import
    # when grow_area, rain_in, and rain_lab are NA this means they don't have a rainfall threshold (aren't in a current cmu)

    # check if lease polygons are valid
    valid_check_list <- st_is_valid(lease_data_albers_join)

    # remove non-valid leases
    lease_data_albers_final <- lease_data_albers_join %>%
      dplyr::mutate(valid_polygon = valid_check_list) %>%
      dplyr::filter(valid_polygon == TRUE) %>%
      dplyr::select(-valid_polygon)

    # invalid lease polygons
    # lease_data_albers_inval <- lease_data_albers_join %>%
    #   dplyr::mutate(valid_polygon = valid_check_list) %>%
    #   dplyr::filter(valid_polygon == FALSE) %>%
    #   dplyr::select(-valid_polygon)
    # only one is invalid (#414)

    # resave centroids b/c lease_data_albers_final has full lease dataset
    lease_data_centroids_albers_final <- lease_data_albers_final %>%
      st_centroid()
    # NA values means that leases don't have a rainfall threshold (aren't in a current cmu)


    # ---- 9. project data ----
    # project data to wgs84 projection
    lease_data_wgs94 <- lease_data_albers_final %>%
      st_transform(crs = wgs84_epsg)

    # project centroids to wgs84 projection
    lease_data_centroid_wgs94 <- lease_data_centroids_albers_final %>%
      st_transform(crs = wgs84_epsg)

    # project data to geojson file type (need this for the web app)
    # lease_data_wgs94_geojson <- sf_geojson(lease_data_wgs94, atomise = FALSE, simplify = TRUE, digits = 5)
    # lease_data_centroid_wgs94_geojson <- sf_geojson(lease_data_centroid_wgs94, atomise = FALSE, simplify = TRUE, digits = 5)
    # lease_data_centroid_wgs94_simple_geojson <- sf_geojson(lease_data_centroid_wgs94_simple, atomise = FALSE, simplify = TRUE, digits = 5)
    # gcp doesn't take in spatial data by default so not doing this for now


    # ---- 10. simplify data for mysql db ----
    # select fields for mysql db
    lease_data_centroid_wgs94_db <- lease_data_centroid_wgs94 %>%
      dplyr::select(lease_id = lease_id,
                    cmu_name,
                    grow_area_name = sga_name,
                    grow_area_desc = sga_desc,
                    rainfall_thresh_in = rain_in)

    # save coordinates
    lease_data_centroid_coords <- as.data.frame(st_coordinates(lease_data_centroid_wgs94_db)) %>%
      dplyr::select(longitude = X, latitude = Y)

    # save lease centroid data as tabular dataset
    lease_data_centroid_wgs94_db_tabular <- lease_data_centroid_wgs94_db %>%
      st_drop_geometry() %>%
      dplyr::bind_cols(lease_data_centroid_coords)


    # ---- 11. export data ----
    lease_bounds_dir <- paste0(lease_data_spatial_output_path, "lease_bounds/")
    lease_centroid_dir <- paste0(lease_data_spatial_output_path, "lease_centroids/")
    dir_list <- list(lease_bounds_dir, lease_centroid_dir)
    for (d in dir_list) {
        # create directory if it doesn't exists
        if (!dir.exists(d)) {
            dir.create(d)
        }
    }

    # export data as shape file for record keeping
    st_write(lease_data_albers_final, paste0(lease_bounds_dir, "lease_bounds_albers.shp"), delete_layer = TRUE)
    st_write(lease_data_centroids_albers_final, paste0(lease_centroid_dir, "lease_centroids_albers.shp"), delete_layer = TRUE)

    # export tabular lease centroids for mysql db
    write_csv(lease_data_centroid_wgs94_db_tabular, paste0(lease_centroid_dir, "lease_centroids_db_wgs84.csv"))

    # export data as geojson for web app
    # write_file(lease_data_wgs94_geojson, paste0(lease_data_spatial_output_path, "lease_bounds/lease_bounds_wgs84.geojson"))
    # write_file(lease_data_centroid_wgs94_geojson, paste0(lease_data_spatial_output_path, "lease_centroids/lease_centroids_wgs84.geojson"))
    # write_file(lease_data_centroid_wgs94_simple_geojson, paste0(lease_data_spatial_output_path, "lease_centroids/lease_centroids_simple_wgs84.geojson"))

}