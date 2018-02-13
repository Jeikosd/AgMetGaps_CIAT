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
bind_raster <- purrr::map(.x = bind_file, .f = raster::raster)

library(future)
library(future.apply)
plan(list(tweak(multisession, workers = 3), tweak(multisession, workers = 4)))

Iizumi_raster <- purrr::map(.x = yield_file, .f = ~future(fraster(.x))) %>%
  future::values()



resample_Iizumi <- purrr::map(.x = Iizumi_raster, .f = ~future(fresample(.x, bind_raster[[1]]))) %>%
  future::values()





bind_raster <- purrr::map(.x = Iizumi_raster, .f = raster)

z <- resample(Iizumi_raster, bind_raster)








#### funciones para make_gaps

## x: files to load raster

fraster <- function(x){
  
  x <- future.apply::future_lapply(X = x, FUN = raster)
  
}

## x: raster
## y: resolution (to make the resample)
fresample <- function(x, y){
  
  x <- future.apply::future_lapply(X = x, FUN = resample, y)
  
}


# x: yield
# y: contenedor climatico

potential <- function(x, y){
  
  x <- raster('//dapadfs/workspace_cluster_9/AgMetGaps/Inputs/iizumi/maize/yield_1981.nc4') %>%
    rotate %>%
    velox()
  
  y <- raster('//dapadfs/workspace_cluster_9/AgMetGaps/Inputs/Yield_Gaps_ClimateBins/maize_yieldgap_netcdf/YieldGap_maize_2000_BaseGDD_8_MaxYieldPct_95_ContourFilteredClimateSpace_10x10_prec_BinMatrix.nc') 
  
  # x <- raster('/mnt/workspace_cluster_9/AgMetGaps/Inputs/iizumi/maize/yield_1981.nc4') %>%
  # rotate %>%
  # velox()
  # y <- raster('/mnt/workspace_cluster_9/AgMetGaps/Inputs/Yield_Gaps_ClimateBins/maize_yieldgap_netcdf/YieldGap_maize_2000_BaseGDD_8_MaxYieldPct_95_ContourFilteredClimateSpace_10x10_prec_BinMatrix.nc') 
  
  # r3 <- overlay(x, y, fun=function(x,y){return(y)})
  points_bind <- rasterToPoints(y) %>%
    tbl_df() %>%
    sf::st_as_sf(coords = c("x","y"))

  z <- x$extract_points(points_bind) %>%
    tbl_df() %>% 
    rename(yield = V1) 
  # z <- raster::extract(x, points_bind[, 1:2]) %>%
  #   tbl_df()
  
  p <- bind_cols(st_coordinates(points_bind) %>% tbl_df(), z, points_bind) %>%
    dplyr::mutate(BinMatrix = as.factor(BinMatrix))
  
  pot <- p %>%
    group_by(BinMatrix) %>%
    summarise(potential = quantile(yield, probs = 0.95, na.rm =  TRUE))
  
  pot_iizumi <- left_join(p, pot, by = 'BinMatrix') %>%
    # dplyr::mutate(potential = ifelse(is.na(yield), NA, potential)) %>%
    dplyr::mutate(potential = if_else(is.na(yield), NA_real_, potential)) %>%
    # dplyr::mutate(potential = case_when( is.na(yield) == NA_real_ ~ NA_real_, TRUE ~ potential))
    dplyr::mutate(gap = potential - yield)
  
  ## rasterize
  # m <- x$as.RasterLayer(band = 1)
  
  coords <- pot_iizumi %>%
    dplyr::select(X, Y) %>%
    data.frame 
  
  potential <- pot_iizumi %>%
    # dplyr::select(!!var) %>%
    dplyr::select(potential) 
  
  gap <- pot_iizumi %>%
    # dplyr::select(!!var) %>%
    dplyr::select(gap) %>%
    pull
  
  na.omit(pot_iizumi)
  potential <- rasterize(coords, y, potential, fun = mean)
  gap <- rasterize(coords, y, gap, fun = mean)
  writeRaster(gap, filename="/mnt/workspace_cluster_9/AgMetGaps/Inputs/iizumi/maize/gap_1981_maize.tif", format="GTiff", overwrite=TRUE)

  
}
