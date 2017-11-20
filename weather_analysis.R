#####################################################################################
##############  01 Make polygons from raster (to use in velox package) ##############
#####################################################################################
## Code to make points to extract using velox package
## crops Rice, Maize, Trigo

library(raster)
library(sp)
library(dplyr)
library(sf)
library(stringr)
library(ncdf4)
library(purrr)
source("main_functions.R")

path <- '/mnt/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/'
planting <- c('Rice.crop.calendar.nc', 'Maize.crop.calendar.nc', 'wheat.crop.calendar.nc')  ## wheat winter???
out_path <- '/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/spatial_points/'    ## folder del proyecto para pegar la informacion necesaria
extent_information <- c(-180,  180,  -50,   50)  ## extent with where is the information (chirps!!!)
raster_source <- 'sacks'

# raster_Polygons(file_nc = paste0(path, planting), out_path, extent = extent_information, raster_source)
file_nc <- paste0(path, planting)
purrr::map(.x = file_nc, .f = raster_Polygons, out_path, extent = extent_information, raster_source)

rm(list = ls())


###
## code to make time series climate from chirps for each point in the calendar polygons

library(lubridate)
library(tidyverse)
library(raster)
library(future)
library(velox)

chirps_path <- '/mnt/data_cluster_4/observed/gridded_products/chirps/daily/'
chirps_file <- list.files(chirps_path, pattern = '.tif$', full.names = T)



raster_files <- chirps_file %>%
  data_frame(file = .) %>%
  mutate(date = purrr::map(.x = file, .f = extract_date)) %>%
  tidyr::unnest() %>%
  mutate(year = lubridate::year(date),
         month = lubridate::month(date),
         day = lubridate::day(date))

x <- raster_files %>%
  filter(year <= 1982) 


# x <- raster_files %>%
#   filter(year <= 1982) %>%
#   pull(file)

x <- x %>%
  base::split(.$year, drop = TRUE) %>%
  purrr::map(.f = filter, month == 1) %>%
  purrr::map(.f = pull, file)

## cargar los puntos que se van a utilizar para extraer


local_cpu <- rep("localhost", availableCores() - 3)
# external_cpu <- rep("caribe.ciat.cgiar.org", 8)  # server donde trabaja Alejandra
external_cpu <- rep("climate.ciat.cgiar.org", each = 4)

workers <- c(local_cpu, external_cpu)

plan(cluster, workers = workers)

# plan(list(tweak(cluster, workers = workers), multicore))

m <- listenv::listenv()
tic("extract")
m <- future::future_lapply(x, FUN = velox_lst, points_coord = points_coord)
toc()

m <- listenv::listenv()
tic("extract")
m <- future::future_lapply(x, FUN = velox_lst, points_coord = points_coord)
toc()


m <- listenv::listenv()
tic("extract")
m <- foreach::foreach(i = 1:length(x), .packages = 'tidyverse') %dopar% {
  
  velox_lst(x[[i]], points_coord = points_coord[1, ])
  
  
  
}
toc()






library(raster)
library(velox)
library(stringr)
library(dplyr)
library(ncdf4)
library(purrr)
library(future)
library(tidyr)
library(lubridate)

# chirps_path <- '/mnt/workspace_cluster_9/AgMetGaps/Chirps/daily/'
chirps_path <- '/mnt/data_cluster_4/observed/gridded_products/chirps/daily/'
  

## cargar raster por dia?
## Generar codigo tanto para extraer .tif como .nc
## tif solo puede contener un atributo, archivos .nc puede contener varios atributos

raster_files <- list.files(chirps_path, pattern = '.tif$', full.names = T) %>%
  data_frame(file = .) %>%
  mutate(date = map(.x = file, .f = extract_date)) %>%
  tidyr::unnest() %>%
  mutate(year = lubridate::year(date)) 

## hacer filtro solo hasta 2016 (o hasta donde se requiera)

raster_files <- raster_files %>%
  filter(year <= 2016) 


### proof tiempo de demora sin paralelo con future

# system.time(
#   raster_files %>%
#     mutate(date = map(.x = file, .f = extract_date)) %>%
#     tidyr::unnest() %>%
#     mutate(velox_raster = map(.x = file, .f = velox))
# )
# user  system elapsed
# 32.205  14.746  98.186

# plan(multisession, workers = availableCores() - 3)  ## para trabajar en una sola maquina
local_cpu <- rep("localhost", availableCores() - 3)
external_cpu <- rep("climate.ciat.cgiar.org", 10)

workers <- c(local_cpu, external_cpu)
plan(cluster, workers = workers)

# system.time(
chirps_df <- raster_files %>%
  mutate(velox_raster = map(.x = file, .f = ~future(velox(.x)))) %>%
  mutate(velox_raster = map(velox_raster, ~value(.x))) 
# )

# user  system elapsed
# 10.370   6.186  52.743

## extraer puntos de planting date
plant_start <- raster(paste0('/mnt/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/Maize.crop.calendar.nc'), varname = 'plant.start')
plant_end <- raster(paste0('/mnt/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/Maize.crop.calendar.nc'), varname = 'plant.end')
  
plant_points <- rasterToPolygons(plant_start,  dissolve = F) 

chirps_df <- chirps_df %>%
  # mutate(points = map2(.x = velox_raster, .y = sp_pdate, .f = extract_velox))
  mutate(points = map(.x = velox_raster, .f = ~future(extract_velox(r = .x, points = plant_points)))) %>%
  mutate(points = map(points, ~value(.x))) 

plant_start <- sack_to_points(plant_start)
plant_end <- sack_to_points(plant_end)

select(chirps_df, date, points) %>%
  unnest() %>%
  purrr::invoke_map(.f = left_join, .x = list(plant_start, plant_end), by = c('lat', 'long'))
  left_join(plant_start, by = c('lat', 'long'))
  
  parallel::stopCluster(cl)


x <- function(){
  
  length_matrix <- scan(file=stdin(), what=integer(), nlines=1)
  
  if (length_matrix >= 100 | length_matrix <= -100) {
    stop("Invalid N by the hackerrank Constraints")
  }
  
  A <- matrix(scan(file=stdin(), nlines = length_matrix), length_matrix, length_matrix, byrow = TRUE)
  
  diag1 <- sum(diag(A))
  
  
  diag2 <- sum(diag(apply(A,2,rev))) 
  
  result <- abs(diag1 - diag2)
  print(result)
  return(result)
  
}

x()

workers <- c("climate.ciat.cgiar.org", "climate.ciat.cgiar.org", "climate.ciat.cgiar.org", "climate.ciat.cgiar.org", "climate.ciat.cgiar.org",
             "localhost", "localhost", "localhost")
cl <- makeClusterPSOCK(workers, revtunnel = TRUE, outfile = "")


f <- list()
for (i in 1:10000) {
  f[[i]] <- future({ rnorm(i, mean = mu, sd = sigma) })
}
v <- values(f)

