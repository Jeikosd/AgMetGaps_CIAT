library(raster)
library(velox)
library(stringr)
library(tidyverse)
library(ncdf4)
# library(sf)


Iizumi <- '/mnt/workspace_cluster_9/AgMetGaps/Inputs/iizumi/'
season <- c("major", "spring")

climate_binds <-'/mnt/workspace_cluster_9/AgMetGaps/Inputs/Yield_Gaps_ClimateBins/'
crops <- c('maize', 'rice', 'wheat')

crops_Iizumi <- list.dirs(Iizumi, recursive = FALSE) %>%
  data_frame(path = . ) %>%
  filter(str_detect(path, paste(season, collapse = '|'))) %>%
  pull(path)

yield_file <- purrr::map(.x = crops_Iizumi, .f = list.files, full.names = T)

bind_file <- list.dirs(climate_binds, recursive = FALSE) %>%
  data_frame(path = . ) %>%
  filter(str_detect(path, paste(crops, collapse = '|'))) %>%
  pull(path)

bind_file <- purrr::map(.x = bind_file, .f = list.files, full.names = T, pattern = '*BinMatrix.nc$')

Iizumi_raster <- purrr::map(.x = yield_file[[1]], .f = raster::raster)
bind_raster <- raster(bind_file[[1]])

## hacer esto en paralelo
purrr::map(.x = Iizumi_raster, .f = raster::resample, bind_raster)




bind_raster <- purrr::map(.x = Iizumi_raster, .f = raster)

z <- resample(Iizumi_raster, bind_raster)
