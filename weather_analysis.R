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
# source("main_functions.R")


# path <- '//dapadfs/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/'
# out_path <- '//dapadfs/workspace_cluster_9/AgMetGaps/weather_analysis/spatial_points/'
out_path <- '/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/spatial_points/'    ## folder del proyecto para pegar la informacion necesaria
path <- '/mnt/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/'


planting <- c('Rice.crop.calendar.nc', 'Maize.crop.calendar.nc', 'wheat.crop.calendar.nc')  ## wheat winter???
extent_information <- c(-180,  180,  -50,   50)  ## extent with where is the information (chirps!!! or AgMerra!!!)
raster_source <- 'sowing_window_sacks'


# raster_Polygons(file_nc = paste0(path, planting), out_path, extent = extent_information, raster_source)
file_nc <- paste0(path, planting)

plant_start <- file_nc %>%
  purrr::map(.f = raster_Polygons, extent = extent_information, varname = 'plant.start')
  # raster_Polygons(file_nc[[1]], extent_information, varname = 'plant.start')
plant_end <- file_nc %>%
  purrr::map(.f = raster_Polygons, extent = extent_information, varname = 'plant.end')
  # raster_Polygons(file_nc[[1]], extent_information, varname = 'plant.end')
vars <- plant_end %>%
  purrr::map_chr(.f = function(x){
    names(x)[3]
    
  })

sowing_window <- purrr::pmap(.l = list(plant_start,
                                plant_end ,
                                vars),
                           .f = bind_cols_sf)

crop_name <- planting %>%
  stringr::str_extract(pattern = '[^.]+')

out_files <- paste0(out_path, crop_name)

## out shp
purrr::pmap(.l = list(sowing_window,
                      out_files,
                      raster_source), 
            .f = write_shp)




rm(list = ls())


###
## code to make time series climate from chirps for each point in the calendar polygons

library(lubridate)
library(tidyverse)
library(raster)
library(future)
library(velox)
library(sf)
library(tictoc)

chirps_path <- '/mnt/data_cluster_4/observed/gridded_products/chirps/daily/'
chirps_file <- list.files(chirps_path, pattern = '.tif$', full.names = T)
points_path <- '/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/spatial_points/Maize'
points_file <- list.files(points_path, pattern = '.geojson$', full.names = T)
out_file <- '/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/precipitation_points/daily_chirps_csv/'


geo_files <- sf::st_read(dsn = points_file) %>%
  as('Spatial')

raster_files <- chirps_file %>%
  data_frame(file = .) %>%
  mutate(date = purrr::map(.x = file, .f = extract_date)) %>%
  tidyr::unnest() %>%
  mutate(year = lubridate::year(date),
         month = lubridate::month(date),
         day = lubridate::day(date))

# x <- raster_files %>%
#   filter(year <= 1982) 


x <- raster_files %>%
  # filter(year <= 1981, month == 1) %>%
  filter(year <= 2016) %>%
  pull(file)

# x <- x %>%
#   base::split(.$year, drop = TRUE) %>%
#   purrr::map(.f = filter, month == 1) %>%
#   purrr::map(.f = pull, file)


# p <- sf::st_read(dsn = points_file)


## cargar los puntos que se van a utilizar para extraer


local_cpu <- rep("localhost", availableCores() - 1)
# external_cpu <- rep("caribe.ciat.cgiar.org", 8)  # server donde trabaja Alejandra
external_cpu <- rep("climate.ciat.cgiar.org", each = 10)

workers <- c(local_cpu, external_cpu)

# plan(multisession, workers = 10)
plan(cluster, workers = workers)

# plan(list(tweak(cluster, workers = workers), multicore))
options(future.globals.maxSize= 891289600)
extract_chirps <- listenv::listenv()
tic("extract the chirps information")
extract_chirps <- future::future_lapply(x, FUN = extract_velox, points = geo_files, out_file) 
toc()


# strategy <- "future::multisession"


#####
##### Code to make weather stations

# plan(strategy, workers = 10)  ## es necesario cambiar el plan?
## mejor ir exportando el anterior objeto???
## ir cargando luego fila a fila el anterior csv para evitar tanto consumo en RAM



library(lubridate)
library(tidyverse)
# library(raster)
library(future)
# library(velox)
# library(sf)
# library(tictoc)
library(fst)
# library(feather)
library(doFuture)
library(data.table)

path <- '/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/precipitation_points/daily_chirps_csv/'
csv_files <- list.files(path = path, full.names = TRUE, pattern = '*.csv$')
out_path <- '/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/precipitation_points/weather_stations/'

## make type of future to read in fread mode an them change to fst o feather and compare this to know wich it is better



local_cpu <- rep("localhost", availableCores() - 2)
# external_cpu <- rep("caribe.ciat.cgiar.org", 8)  # server donde trabaja Alejandra
external_cpu <- rep("caribe.ciat.cgiar.org", each = 5)

# workers <- c(local_cpu, external_cpu)

options(future.globals.maxSize = 31912896000) # (31912896000)~31 Gb
# extract_Wstation(csv_files, cpus = workers , strategy = "future::cluster")

# plan(multisession, workers = 10)



# plan(cluster, workers = workers)
plan(cluster, workers = local_cpu)


future::future_lapply(csv_files, FUN = make_fst)

### otra parte

library(future)
library(fst)
library(dplyr)
library(tictoc)
library(data.table)


path <- '/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/precipitation_points/daily_chirps_csv/'
fst_files <- list.files(path = path, full.names = TRUE, pattern = '*.fst$')
# csv_files <- list.files(path = path, full.names = TRUE, pattern = '*.csv$')
# n_pixel <- read.fst(fst_files[1]) %>%
  # dim() %>%
  # magrittr::extract(1)
 
# seq_pixel <- 1:n_pixel


options(future.globals.maxSize = 3191289600) 


local_cpu <- rep("localhost", availableCores() - 2)
external_cpu <- c(rep("climate.ciat.cgiar.org", each = 7))
workers <- c(local_cpu, external_cpu)
plan(cluster, workers = workers )
# plan(multisession, workers = length(local_cpu))

x <- future::future_lapply(x = fst_files, FUN = fst::read.fst, as.data.table = TRUE)

tic("future lapply weather station chirps")
y <- future::future_lapply(x = x, FUN = fst_pixel, pixel = 1)  # 1.4 minutos teniendo cargado la informacion
toc()

tic("weather station chirps")
map(.x = x, .f = fst_pixel, pixel = 1) # 0.2 muinutos mas rapido que en paralelo
toc()



tic("weather station chirps")
map(.x = x, .f = ~future(fst_pixel(x = .x, pixel = 1))) %>%   ## funcion muy lenta
  values()
toc()




map(.x = x, .f = fst_pixel, pixel = 2)
tic("mapping weather station chirps")
x <- map(.x = fst_files[1:100], .f = ~future(make_Wstation(pixel = .x, x = x))) %>%
  future::values()
toc()








make_Wstation(x, pixel = 1)

export_weather(x, pixel = 1)

fst_pixel(fst_files[2], pixel  = 1)

tic("fst one pixel")
x <- map(.x = fst_files[1:1000], .f = fst_pixel, pixel = 1)
toc()

tic("readr")
map(.x = csv_files[1:100], .f = read_daily, pixel = 1)
toc()

tic("fst")
x <- map(.x = fst_files[1:1000], .f = read_fst)
toc()


fst_pixel(x, pixel  = 1)
tic("readr")
x <- lapply(csv_files[1:1000], FUN = read_csv)
toc()


read_daily <- function(x, pixel){
  
  readr::read_csv(file = x, skip = pixel, n_max = 1, col_types = cols()) ## this works

  
}

# fst_df <- read.fst(fst_files[1])

# df <- read.fst(paste0(path, "dataset.fst"))








v <- listenv()
for (i in 1:1000) { v[[i]] %<-% {
  
 
    mean(rnorm(100))
  }
  
  
}


for (i in 1:1000) {
  
  
  v[i] <- mean(rnorm(100))
}



m <- listenv::listenv()
# tic("extract")
m <- future::future_lapply(x, FUN = velox_lst, points_coord = points_coord, out_file)
# toc()

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







###
## code to make time series climate from chirps for each point in the calendar polygons

library(lubridate)
library(tidyverse)
library(raster)
library(future)
library(velox)
library(sf)
library(tictoc)
library(future.apply)

chirps_path <- '/mnt/data_cluster_4/observed/gridded_products/chirps/daily/'
chirps_file <- list.files(chirps_path, pattern = '.tif$', full.names = T)
points_path <- '/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/spatial_points/Maize'
points_file <- list.files(points_path, pattern = '.geojson$', full.names = T)
out_file <- '/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/precipitation_points/daily_chirps_csv/'


geo_files <- sf::st_read(dsn = points_file) 


raster_files <- chirps_file %>%
  data_frame(file = .) %>%
  mutate(date = purrr::map(.x = file, .f = extract_date)) %>%
  tidyr::unnest() %>%
  mutate(year = lubridate::year(date),
         month = lubridate::month(date),
         day = lubridate::day(date))

# x <- raster_files %>%
#   filter(year <= 1982) 


x <- raster_files %>%
  # filter(year <= 1981, month == 1) %>%
  filter(year <= 2016) %>%
  pull(file)


# x <- x %>%
#   base::split(.$year, drop = TRUE) %>%
#   purrr::map(.f = filter, month == 1) %>%
#   purrr::map(.f = pull, file)


# p <- sf::st_read(dsn = points_file)


## cargar los puntos que se van a utilizar para extraer


local_cpu <- rep("localhost", availableCores() - 1)
external_cpu <- rep("caribe.ciat.cgiar.org", 8)  # server donde trabaja Alejandra
# external_cpu <- rep("climate.ciat.cgiar.org", each = 10)

workers <- c(local_cpu, external_cpu)
options(future.globals.maxSize= 891289600)
# plan(multisession, workers = 10)
plan(cluster, workers = workers)

# plan(list(tweak(cluster, workers = workers), multicore))

extract_chirps <- listenv::listenv()
tic("extract the chirps information")
extract_chirps <- future::future_lapply(x, FUN = extract_velox, points = geo_files, out_file) 
toc()


# strategy <- "future::multisession"




mean_point <- function(x){
  
  x[x<0] <- NA
  mean(x, na.rm = T)
  
}


extract_velox <- function(file, points, out_file){
  
  file <- x[1:600]
  points <- geo_files
  
  
  # velox(list(velox(x[1], velox[2])))
  # tic("parallel future")
  # vx_raster <- future.apply::future_lapply(X = file, FUN = velox::velox) %>%
  # velox()
  
  
  # toc()
  
  
  # velox(list(velox(x[1], velox[2])))
  # tic("parallel future")
  # vx_raster <- future.apply::future_lapply(X = file, FUN = raster) %>%
  # velox()
  
  
  # toc()
  
  
  
  # tic('unparalelized stack')
  # stk <- purrr::map(.x = file, .f = raster) %>%
  #   raster::stack()
  # 
  # vx_raster <- velox::velox(stk)
  # toc()
  
  # tic('unparalelized velox list')
  # stk <- purrr::map(.x = file, .f = velox::velox) %>%
  #   velox()
  # 
  # toc()
  
  
  ## paralelizar esta parte
  # plan(multisession)
  velox(file[1], extent=c(0,1,0,1))
  tic('parallel map velox')
  vx_raster <- purrr::map(.x = file, .f = ~future(velox(.x))) %>%
    future::values() %>%
    velox()
  toc()
  
  # vx_raster <- velox::velox(stk)
  
  
  # coords <- sp::coordinates(points) %>%
  #   tbl_df() %>%
  #   dplyr::rename(lat = V1, 
  #                 long = V2)
  
  coords <- points  %>% 
    st_set_geometry(NULL) %>%
    dplyr::select(lat, long) %>%
    as_tibble()
  
  
  # 
  date_raster <- purrr::map(.x = file, .f = extract_date) %>%
    purrr::map(.x = ., .f = as_tibble) %>%
    bind_rows() %>%
    pull()
  # mutate(id = as_factor(paste0('day_', 1:nrow(.)))) %>%
  # tidyr::spread(id, value) %>%
  
  
  
  ## agregarle lat y long mejor
  tic('unparallelized extract velox')
  values <- vx_raster$extract(points, fun = mean_point) %>%
    tbl_df() %>%
    purrr::set_names(date_raster) %>%
    mutate(id = 1:nrow(.))
  toc()
  
  ## podemos hacer esta parte en paralelo
  fast_extract <- function( points, x, fun){
    x$extract(points, fun = fun) %>%
      tbl_df()
  }
  
  
  fast_extract(y, points[1, ], fun = mean_point)
  
  future.apply::future_lapply(X = points, FUN = fast_extract, y, mean_point)
  z <- purrr::map(.x = points, .f = ~future(fast_extract(y, .x, fun = mean_point))) %>%
    future::values() 
  format(object.size(y), units = "GiB")
  
  
  ##
  ## agregacion por pixel
  
  coords <- filter(coords, row_number() == 1)
  by_pixel <- filter(values, row_number() == 1) %>%  
    tidyr::gather(date, precip, -id) %>%
    cbind(coords)
  
  
  
  # column_names <- dplyr::tbl_vars(values) 
  
  write_csv(values, path = paste0(out_file, daily_day, '.csv'))
  
  return(values)
}
