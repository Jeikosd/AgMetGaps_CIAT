## weather analysis

library(raster)
library(velox)
library(stringr)
library(dplyr)
library(ncdf4)
library(purrr)
library(future)

chirps_path <- '/mnt/workspace_cluster_9/AgMetGaps/Chirps/daily/'

## cargar raster por dia?
## Generar codigo tanto para extraer .tif como .nc
## tif solo puede contener un atributo, archivos .nc puede contener varios atributos

raster_files <- list.files(chirps_path, pattern = '.tif$', full.names = T) %>%
  data_frame(file = .)


extract_date <- function(filename){
  
  stringr::str_extract(filename, "\\d{4}.\\d{2}.\\d{2}") %>%
    stringr::str_replace_all(pattern = '[:punct:]', replacement  = '-') %>%
    as.Date(format = '%Y-%m-%d') %>%
    magrittr::extract2(1)
}

extract_velox <- function(r, points){
  
  # r <- velox('/mnt/workspace_cluster_9/AgMetGaps/Chirps/daily//chirps-v2.0.1981.01.01.tif')
  # points <- sp_pdate
  
  
  coords <- coordinates(points) %>%
    tbl_df() %>%
    rename(lat = V1, long = V2)
  
  r$extract(points, fun = mean) %>%
    tbl_df() %>%
    rename(values = V1) %>%
    bind_cols(coords) %>%
    dplyr::select(lat, long, values)
  
}

### proof tiempo de demora sin paralelo con future

system.time(
  raster_files %>%
    mutate(date = map(.x = file, .f = extract_date)) %>%
    tidyr::unnest() %>%
    mutate(velox_raster = map(.x = file, .f = velox))
)
user  system elapsed
32.205  14.746  98.186

plan(multisession, workers = availableCores() - 3)


system.time(
y <- raster_files %>%
  mutate(date = map(.x = file, .f = extract_date)) %>%
  tidyr::unnest() %>%
  mutate(velox_raster = map(.x = file, .f = ~future(velox(.x)))) %>%
  mutate(velox_raster = map(velox_raster, ~value(.x))) 
)

user  system elapsed
10.370   6.186  52.743

pdate <- raster(paste0('/mnt/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/Maize.crop.calendar.nc'), varname = 'plant.start') 
sp_pdate <- rasterToPolygons(pdate,  dissolve = F) 

y <- y %>%
  # mutate(points = map2(.x = velox_raster, .y = sp_pdate, .f = extract_velox))
  mutate(points = map(.x = velox_raster, .f = ~future(extract_velox(r = .x, points = sp_pdate)))) %>%
  mutate(points = map(points, ~value(.x))) 


select(y, date, points) %>%
  unnest()
