library(raster)
library(velox)
library(stringr)
library(tidyverse)
library(ncdf4)
library(sf)
library(velox)
library(tidyverse)
library(stringr)
library(future)
library(future.apply)
library(raster)
library(glue)



dir_path <- '/mnt/workspace_cluster_9/AgMetGaps/'  # Main folder that contains all files and sub-folders
out_path <-'/home/jmesa/'  ## folder server (to avoid writing to disk)



## the next directories depends of the main path
## Settings Iizumi paths
Iizumi <- 'Inputs/iizumi/'
crops <- c('maize', 'rice', 'wheat')
season <- c("major", "spring")
## setting Climate Bins
climate_binds <- 'Inputs/Yield_Gaps_ClimateBins/'



# Iizumi <- '/mnt/workspace_cluster_9/AgMetGaps/Inputs/iizumi/'
Iizumi <- glue::glue('{dir_path}{Iizumi}')
# climate_binds <-'/mnt/workspace_cluster_9/AgMetGaps/Inputs/Yield_Gaps_ClimateBins/'
climate_binds <- glue::glue('{dir_path}{climate_binds}')


crops_Iizumi <- list.dirs(Iizumi, recursive = FALSE) %>%
  data_frame(path = . ) %>%
  filter(str_detect(path, paste(season, collapse = '|'))) %>%
  pull(path)

yield_file <- purrr::map(.x = crops_Iizumi, .f = list.files, full.names = T, pattern = '*.nc4$')

bind_file <- list.dirs(climate_binds, recursive = FALSE) %>%
  data_frame(path = . ) %>%
  filter(str_detect(path, paste(crops, collapse = '|'))) %>%
  pull(path)

bind_file <- purrr::map(.x = bind_file, .f = list.files, full.names = T, pattern = '*BinMatrix.nc$')


# 
# fraster <- function(x){
# 
#   x <- future.apply::future_lapply(X = x, FUN = rotate_raster)
#   
# }

## rotar ya que Iizumi lo necesita
rotate_raster <- function(x){
  
  x <- raster(x) %>%
    raster::rotate(x, overwrite = T)
}

##
out_names <- function(x, type){
  
  # x <- "/mnt/workspace_cluster_9/AgMetGaps/Inputs/iizumi//maize_major/yield_1981"
  
  glue("{str_extract(x, pattern = '^[^.]*')}{type}")
  
  
  
}

mkdirs <- function(fp) {
  
  if(!file.exists(fp)) {
    mkdirs(dirname(fp))
    dir.create(fp)
  }
  
} 

            
  

# out_potential <- out_names(out_name, type = '_potential.tif')
# out_gap <- out_names(basename(yield_path), type = '_gap.tif')

# dirname(out_name)
# out_path <- '/mnt/workspace_cluster_9/AgMetGaps/gaps/'





make_gap <- function(yield_path, bind, out_path){
  
  # yield_path <- yield_path[i]
  # bind <- bind_path
  # out_path
  # plan(sequential)
  
  ## esta parte no es necesario hacerla en paralelo
  
  # yield <- fraster(yield_path)
  
  out_names <- function(x, type){
    
    # x <- "/mnt/workspace_cluster_9/AgMetGaps/Inputs/iizumi//maize_major/yield_1981"
    
    glue::glue("{stringr::str_extract(x, pattern = '^[^.]*')}{type}")
    
    
    
  }
  
  yield <- rotate_raster(yield_path)
  bind <- raster(bind)
  ## esta parte es lenta

  points_bind <- bind %>%
    rasterToPoints() %>%
    tbl_df() %>%
    sf::st_as_sf(coords = c("x","y"))
  
  yield <- velox(yield)
  
  yield_points <- yield$extract_points(points_bind) %>%
    tbl_df() %>% 
    rename(yield = V1) 
  # z <- raster::extract(x, points_bind[, 1:2]) %>%
  #   tbl_df()
  
  yield_by_bind <- bind_cols(st_coordinates(points_bind) %>% tbl_df(), yield_points, points_bind) %>%
    dplyr::mutate(BinMatrix = as.factor(BinMatrix))
  
  potential <- yield_by_bind %>%
    group_by(BinMatrix) %>%
    summarise(potential = quantile(yield, probs = 0.90, na.rm =  TRUE))
  
  gap_analysis <- left_join(yield_by_bind, potential, by = 'BinMatrix') %>%
    # dplyr::mutate(potential = ifelse(is.na(yield), NA, potential)) %>%
    dplyr::mutate(potential = if_else(is.na(yield), NA_real_, potential)) %>%
    # dplyr::mutate(potential = case_when( is.na(yield) == NA_real_ ~ NA_real_, TRUE ~ potential))
    dplyr::mutate(gap = potential - yield)
  
  ## rasterize
  # m <- x$as.RasterLayer(band = 1)
  
  coords <- gap_analysis %>%
    dplyr::select(X, Y) %>%
    data.frame 
  
  potential <- gap_analysis %>%
    # dplyr::select(!!var) %>%
    dplyr::select(potential) %>%
    pull
  
  gap <- gap_analysis %>%
    # dplyr::select(!!var) %>%
    dplyr::select(gap) %>%
    pull
  
  
  potential <- rasterize(coords, bind, potential, fun = mean)
  gap <- rasterize(coords, bind, gap, fun = mean)
  
  
  # glue("{out_path}{out_names(basename(yield_path), type = '_potential.tif')}")
  
  
  out_data <- str_replace(string = glue::glue('{yield_path}'),
                          pattern = dir_path, replacement = out_path) %>%
    dirname()
  

  
  
  mkdirs(out_data)
  

  out_potential <- glue("{out_data}/{out_names(basename(yield_path), type = '_potential.tif')}")
  out_gap <- glue("{out_data}/{out_names(basename(yield_path), type = '_gap.tif')}")

  writeRaster(potential, filename = out_potential, format = "GTiff", overwrite = TRUE)
  writeRaster(gap, filename = out_gap, format = "GTiff", overwrite = TRUE)
  
  rm('points_bind', 'yield', 'yield_by_bind', 'gap_analysis', 'coords', 'potential', 'gap')
  gc(reset = T)
  
  cat(glue('finished {out_potential}'))
  
}

future.apply::future_lapply(X = 1:length(yield_file[[1]]), FUN = rotate_raster)
yield_file
bind_file
## que salgan con el nombre del cultivo

run_gap <- function(yield_path, bind_path, out_path){
  
  # plan(sequential)
  # plan(multisession, workers = 10)
  # yield_path <- yield_file[[1]]
  # bind_path <- bind_file[[1]]

  future.apply::future_lapply(X = 1:length(yield_path), FUN = function(i) make_gap(yield_path[i],
                                                                                    bind_path,
                                                                                    out_path))
  


  
}

options(future.globals.maxSize= 8912896000)
plan(multisession, workers = 9)
purrr::map2(.x = yield_file, .y = bind_file, .f = run_gap, out_path)

