# ---- script header ----
# script name: ncdmf_tidy_state_bounds_script.R
# purpose of script: tidying regional and state bounds for later calcs
# author: sheila saia
# date created: 20200604
# email: ssaia@ncsu.edu

# ----- Seed data -----
# /spatial/inputs/state_bounds_data/state_bounds_raw/State_Boundaries.shp

# ----- Creates directory and save outputs -----
# [Added directory]
# /spatial/inputs/state_bounds_data/state_bounds

# [Outputs]
# 1. {state_abbrev}_bounds_10kmbuf_albers.shp
# 2. {state_abbrev}_bounds_10kmbuf_wgs84.shp
# 3 state_bounds_albers.shp
# 4. state_bounds_wgs84.shp

#  SEE FixMe? - projection


dmf_tidy_state_bounds <- function(state_bounds_spatial_data_input_path, state_bounds_spatial_data_output_path, state_name, state_abbrev, state_bounds_shp) {

    # ---- 1. load libraries ----=
    # load libraries
    library(tidyverse)
    library(sf)
#     library(here)

    # ---- 2. define base paths ----
    # base path to data
    # data_base_path = "...analysis/data/" # set this and uncomment!
    # data_base_path = "/Users/sheila/Documents/bae_shellcast_project/shellcast_analysis/web_app_data/"
#     data_base_path = here::here("data")

    # ---- 3. use base paths and define projections ----
    # path to state bounds spatial inputs
#     state_bounds_spatial_data_input_path <- paste0(data_base_path, "/spatial/inputs/state_bounds_data/state_bounds_raw/")

    # path to state bounds spatial outputs
#     state_bounds_spatial_data_output_path <- paste0(data_base_path, "/spatial/inputs/state_bounds_data/state_bounds/")

    # define epsg and proj4 for N. America Albers projection (projecting to this)
#     na_albersna_albers_proj4_proj4 <- "+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"

    # FixMe?:
    # Originally it was 102008 but it didn't work so changed to ESRI:102008
    # I am not sure ESRI:102008 project slightly different from 102008
    na_albers_epsg <- 'ESRI:102008'

    # define wgs 84 projection
    wgs84_epsg <- 4326


    # ---- 3. load data ----
    # state boundaries spatial data
    state_bounds_raw <- st_read(paste0(state_bounds_spatial_data_input_path, state_bounds_shp))


    # ---- 4. check spatial data projections and project ----
    # check state bounds
    st_crs(state_bounds_raw)
    # epsg 5070 (albers conic equal area)

    # project to na albers
    state_bounds_albers <- state_bounds_raw %>%
      dplyr::select(OBJECTID:STATE_NAME) %>%
      st_transform(crs = na_albers_epsg)
    # st_crs(state_bounds_albers)
    # it checks!

    # project to wgs84
    state_bounds_wgs84 <- state_bounds_raw %>%
      dplyr::select(OBJECTID:STATE_NAME) %>%
      st_transform(crs = wgs84_epsg)
    # st_crs(state_bounds_wgs84)
    # it checks!

    # create directory if it doesn't exists
    if (!dir.exists(state_bounds_spatial_data_output_path)) {
        dir.create(state_bounds_spatial_data_output_path)
    }

    # export data
    st_write(state_bounds_albers, paste0(state_bounds_spatial_data_output_path, "state_bounds_albers.shp"), delete_layer = TRUE)
    st_write(state_bounds_wgs84, paste0(state_bounds_spatial_data_output_path, "state_bounds_wgs84.shp"), delete_layer = TRUE)


    # ---- 5. select only nc bounds and buffer ----
    # select and keep only geometry
    bounds_gem_albers <- state_bounds_albers %>%
      filter(STATE_NAME == state_name) %>%
      st_geometry() %>%
      st_simplify()

    # keep a copy projected to wgs84 too
    bounds_geom_wgs84 <- bounds_gem_albers %>%
      st_transform(crs = wgs84_epsg)

    # export
    # st_write(bounds_gem_albers, paste0(state_bounds_spatial_data_output_path, "nc_bounds_albers.shp"), delete_layer = TRUE)
    # st_write(bounds_geom_wgs84, paste0(state_bounds_spatial_data_output_path, "nc_bounds_wgs84.shp"), delete_layer = TRUE)


    # ---- 6. buffer bounds ----

    # buffer 5 km
    # nc_bounds_5kmbuf_albers <- bounds_gem_albers %>%
    #  st_buffer(dist = 5000) # distance is in m so 5 * 1000m = 5km

    # buffer 10 km
    bounds_10kmbuf_albers <- bounds_gem_albers %>%
      st_buffer(dist = 10000) # distance is in m so 10 * 1000m = 10km

    # save a copy projected to wgs84
    bounds_10kmbuf_wgs84 <- bounds_10kmbuf_albers %>%
      st_transform(crs = wgs84_epsg)

    # export
    state_abbrev_lc <- tolower(state_abbrev)
    # st_write(nc_bounds_5kmbuf_albers, paste0(state_bounds_spatial_data_output_path, "nc_bounds_5kmbuf_albers.shp"), delete_layer = TRUE)
    st_write(bounds_10kmbuf_albers, paste0(state_bounds_spatial_data_output_path, state_abbrev_lc, "_bounds_10kmbuf_albers.shp"), delete_layer = TRUE)
    st_write(bounds_10kmbuf_wgs84, paste0(state_bounds_spatial_data_output_path, state_abbrev_lc, "_bounds_10kmbuf_wgs84.shp"), delete_layer = TRUE)

}
