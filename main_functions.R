## main functions

## first part

# fp: folder to make

mkdirs <- function(fp) {
  
  if(!file.exists(fp)) {
    mkdirs(dirname(fp))
    dir.create(fp, showWarnings = F, mode = "777")
    
  }
  
  return(fp)
} 



## x: a raster object
## extent: extent to crop the raster

make_polygons <- function(x, extent){
  
  temp_raster <- rasterTmpFile()
  
  exchange <- names(x)
  variable <- stringr::str_replace_all(names(x), pattern = '[:punct:]+', replacement  = '_')
  
  points <- x %>%
    raster::crop(extent) %>%
    rasterToPoints() %>%
    dplyr::tbl_df() %>%
    dplyr::select(x, y) %>%
    dplyr::rename(lat = y, long = x)
  
  polygons <- x %>%
    raster::crop(extent) %>%
    raster::writeRaster(filename = temp_raster, overwrite = TRUE) %>%
    raster::rasterToPolygons(dissolve = F) 
  
  polygons@data = data.frame(polygons@data, data.frame(points)) %>%
    dplyr::tbl_df() %>%
    dplyr::rename(!!variable :=  !!exchange) %>%
    dplyr::select(lat, long, !!variable)
  
  
  return(polygons)
  # x <- plant_start
  
  
}



## funcion para crear puntos como poligonos (formato que necesita el paquete velox para extraer puntos de raster)

# file_nc: sacks nc planting (or a raster file)
# extent: extent if it is necessary to crop 

raster_Polygons <- function(file_nc, extent, ...){
  
  # file_nc <- file_nc[[1]]
  # out_path
  # extent <- extent_information
  
  # crop_name <- stringr::str_extract(basename(file_nc), pattern = '[^.]+')
  
  r <- raster::raster(file_nc, ...)
  
  # var_name <- names(r) %>%
    # stringr::str_replace_all(pattern = '[:punct:]+', replacement  = '_')
  
  # start_planting <- raster::raster(file_nc, varname = 'plant.start')
  # end_planting <- raster::raster(file_nc, varname = 'plant.end')
  

  points_df <- make_polygons(r, extent)
  # setwd(out_path)
  points_df <- sf::st_as_sf(points_df)
  
  return(points_df)
  # as(points_df, "Spatial")

}

## x: a sf object
## y: a sf object
## vars: variables in y to bind cols in x

bind_cols_sf <- function(x, y, vars){
  
  
  y <- data.frame(y) %>%
         dplyr::tbl_df() %>%
         dplyr::select(!!vars)
  
  z <- dplyr::bind_cols(x, y)
  
  return(z)

}
  
## sf: now a sf object

write_shp <- function(sf, out_path, raster_source){
    
  
  ## sf <- sowing_window[[1]]
  ## out_path <- out_files[1]
  
    out_points <- mkdirs(fp = out_path)
    
    # sf::st_write(sf, dsn = paste0(out_points, '_', raster_source, '.GeoJSON'))
    # sf::st_write(sf, paste0(out_points, '_', raster_source, '.csv'), layer_options = "GEOMETRY=AS_XY")
    # sf::st_write(sf, paste0(out_points, '_', raster_source, '.csv'), layer_options = "GEOMETRY=AS_XY", delete_layer = TRUE)
    # sf::st_write(sf, paste0(out_points, '_', raster_source, '.csv'), layer_options = "GEOMETRY=AS_WKT", delete_dsn = TRUE)
    # 
    # st_read("crime_wkt.csv", options = "GEOM_POSSIBLE_NAMES=WKT")
    # st_read(dsn = paste0(out_points, '_', raster_source, '.GeoJSON'))
    # sf::st_read(dsn = paste0(out_points, '/_', raster_source, '.csv'), options = "GEOM_POSSIBLE_NAMES=WKT")
    # st_write(sf, dsn = paste0(out_points, '/_', raster_source, '.csv'), layer_options = "GEOMETRY=AS_WKT", delete_layer = TRUE, delete_dsn = TRUE) 
    # st_write(sf, dsn = paste0(out_points, '/_', raster_source), "PG:dbname=postgis", "sids", layer_options = "OVERWRITE=true")
    # 
    
    file <- paste0(basename(out_path), '_', raster_source, '.geojson')
    sf::st_write(sf, dsn = paste(out_points, file, sep = '/'),
             # layer = "rice_points_sacks.shp",
             # layer = paste0(basename(out_path), '_', raster_source, '.geojson'),
             driver = "GeoJSON",
             delete_layer = TRUE,
             delete_dsn = TRUE)
  
}


############################################
#### second part ###########################
############################################
  
  
extract_date <- function(filename){
  
  # filename <- '/mnt/data_cluster_4/observed/gridded_products/chirps/daily//chirps-v2.0.1983.09.25.tif'
  filename %>%
    stringr::str_extract("\\d{4}.\\d{2}.\\d{2}") %>%
    stringr::str_replace_all(pattern = '[:punct:]', replacement  = '-') %>%
    as.Date(format = '%Y-%m-%d') %>%
    magrittr::extract2(1)
  
}





mean_point <- function(x){
  
  x[x<0] <- NA
  mean(x, na.rm = T)
  
}


extract_velox <- function(file, points, out_file){
  
  # file <-x[2]
  # points <- geo_files
  
  vx_raster <- velox::velox(file)
  
  
  coords <- sp::coordinates(points) %>%
    tbl_df() %>%
    dplyr::rename(lat = V1, 
                  long = V2)
  # 
  date_raster <- extract_date(file) %>%
    rep(dim(coords)[1])
  date_raster <- data_frame(date_raster)
  
  daily_day <- distinct(date_raster, date_raster) %>% 
    pull()

  # 
  # # plan(multisession)
  # ff_path <- paste0('/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/precipitation_points/', 'points.ffdata')
  # #
  values <- vx_raster$extract(points, fun = mean_point) %>%
    tbl_df() %>%
    dplyr::rename(precip = V1) %>%
    dplyr::bind_cols(coords, date_raster) 
  
  write_csv(values, path = paste0(out_file, daily_day, '.csv'))
  # data.table
  
  # prueba <- list(values, values2)
  # prueba[[1]]
  # prueba <- ff(vmode = "double", dim = dim(values), filename = ff_path)
  # prueba <- as.ffdf(prueba[[1]], filename = ff_path)
  # purrr::map(.x = prueba[[1]], .y = prueba[[2]], .f = bind_rows)
  # as.ffdf.data.frame(values, filename = ff_path)
  
  # bind_rows(prueba)
  # return(values)
  
  # bind_rows_raster <- function(x, y){
    # x %>%
      # filter(row_number()==y)
  # }
  
  # purrr::map(.x = list(values, values2), .f = bind_rows_raster, y = 1) %>%
    # bind_rows
  return(values)
}

## Make weather station for each latitude and longitude
## x: la lista extraida de chirps para cada dia
## para seleccionar el pixel para posteriormente organizar la serie climatica

# select_pixel <- function(x, pixel){
#   
#   x[pixel, ]
#   
# }

## Making weather station for each latitude and longitude

# make_Wstation <- function(x, pixel){
#   
#   x <- purrr::map(.x = x, .f = select_pixel, pixel) %>%
#     dplyr::bind_rows()
# }




## Mejorar esta funcion

make_fst <- function(x){
  
  # x <- csv_files[1]
  date <- basename(x)
  path <- stringr::str_replace_all(x, pattern = date, replacement  = '')
    
  date <- stringr::str_replace_all(date, pattern = ".csv", replacement  = '')
  
  x <- data.table::fread(x) %>%
    as.data.frame()
  
  # x <- as.data.frame(x)

  fst::write.fst(x, paste0(path, date, ".fst"))
}

# make_fst(csv_files[1])

# x <- fst_files[1]

fst_pixel <- function(x, pixel){
  
  # fst::read.fst(x, from = pixel, to = pixel, as.data.table = TRUE)
  x[pixel, ]
}

# fst_pixel(x = fst_files[1], pixel = 2)
## paralelizar esta parte

# map(.x = fst_files, .f = fst_pixel, pixel = 1)

make_Wstation(pixel = 1, x = fst_files[1:100])
tic("paralelizacion map")
purrr::map(.x = fst_files, .f = ~future(fst_pixel(x = .x, 1)))
future:future_lapply(x = fst_files, FUN = fst_pixel, pixel = 1)

make_Wstation <- function(pixel, x){
  
  
  # pixel <- 1
  # col_names <- df_names
  
  # tic("make weather station using readr")
  purrr::map(.x = x, .f = fst_pixel, pixel) %>%
    # rbindlist()
    bind_rows()
  # toc()
  
  # fread_folder(path, extension = "CSV", skip = 1)
  
  
}

export_weather <- function(x, pixel){
  
  
  weather_station <- make_Wstation(x, pixel)
  # name_station <- paste0(out_path, 'daily_pixel_', pixel, '.csv')
  # write_csv(x = weather_station, path = name_station) 
  
}



read_daily <- function(x, skip, col_names){
  
  readr::read_csv(file = x, skip = skip, n_max = 1, col_names = col_names, col_types = cols()) ## this works
  # text <- read_lines(x, skip = skip, n_max = 1) 
  # text <- paste(col_names, collapse = ',') %>%
  #   paste(text, sep = '\n')
                  
  
  # read_csv(paste(paste(col_names, collapse = ','), text, sep = '\n'))
  
  
  # readr::read_csv(file = x[1], skip = 2, n_max = 1, col_names = col_names, col_types = cols())
  

  
  # data.table::fread(file = x, skip = skip, nrows = 1, col.names = df_names)
  
}

make_Wstation <- function(x, pixel, col_names){
  

  # pixel <- 1
  # col_names <- df_names
  
  # tic("make weather station using readr")
  purrr::map(.x = x, .f = read_daily, skip = pixel, col_names = col_names) %>%
    # rbindlist()
    bind_rows()
  # toc()
  
  # fread_folder(path, extension = "CSV", skip = 1)
  
  
}

export_weather <- function(x, pixel, out_path){
  
  df_names <- readr::read_csv(x[1], n_max = 0, col_types = cols()) %>% 
    names()
  
  weather_station <- make_Wstation(x, pixel , col_names = df_names)
  name_station <- paste0(out_path, 'daily_pixel_', pixel, '.csv')
  write_csv(x = weather_station, path = name_station) 
  
}



# select_pixel <- function(pixel, x){
#   
#   purrr::map(.x = x, .f = function(x, pixel) x[pixel, ], pixel = pixel) %>%
#     dplyr::bind_rows()
#   
#   
# }

# make_Wstation <- function(pixel, x){
#   
#   # pixel <- 1:20
#   # x 
#   # plan(multisession, workers = cpus)
#   x <- purrr::map(.x = pixel, ~future(select_pixel(pixel = .x, x))) %>%
#     future::values()
#  
# }


extract_Wstation <- function(x, cpus, strategy){
  
  num_pixel <- purrr::map(.x = x, .f = function(x) dim(x)[1]) %>%
    unlist %>%
    unique()
  
  
  # cpus <- future::availableCores() - 3
  
  # x <- make_Wstation(pixel = 1:num_pixel, x, out_path)
  # is it better foreach?
  registerDoFuture() 
  plan(strategy, workers = cpus)
  
  foreach(i = 1:length(x)) %dopar% {
    
    export_weather(pixel = i, x, out_path) 
  }
  
  # strategy <- "future::multisession"
  

  
}


export_weather <- function(x, path){
  
  # path <- '/mnt/workspace_cluster_9/AgMetGaps/weather_analysis/precipitation_points/'
  file_name <- paste0(path, 1:length(x), '.csv')
  
  # purrr::map2(.x = prueba, .y = file_name, ~future(write_csv(x = .x, path = .y)))
  f <- listenv::listenv()
  f <- future({ for(i in 1:length(prueba)){
    
    write_csv(x = prueba[[i]], path = file_name[i]) 
  }
  }) 
  
  while (!resolved(f)) {
    cat("Making climate csv")
    cat("\n")
  }

}

for(i in 1:500){
  # print(i)
  
  future({ for(i in 1:1000){
    
    write_csv(x = prueba[[i]], path = file_name[i]) 
  }
    }) 
 
  
}



velox_year <- function(data){
  
  data %>%
    mutate(vx_raster = purrr::map(.x = file, .f = velox)) %>%
    dplyr::select(vx_raster) %>%
    magrittr::extract2(1)
  
}

velox_year <- function(data){
  
  data %>%
    # mutate(vx_raster = purrr::map(.x = file, .f = ~future(velox(.x)))) %>%
    mutate(vx_raster = purrr::map(.x = file, .f = function(.x){
      
      y <- listenv()
      y %<-% {
        
        velox(.x)
      }
      
    })) %>%
    # mutate(vx_raster = purrr::map(.x = vx_raster, .f = ~value)) %>%
    dplyr::select(vx_raster) %>%
    magrittr::extract2(1)
  
}



extract_velox <- function(file, points){
  
  # r <- velox('/mnt/workspace_cluster_9/AgMetGaps/Chirps/daily//chirps-v2.0.1981.01.01.tif')
  # points <- plant_points
  
  
  vx_raster <- velox(file)
  
  coords <- coordinates(points) %>%
    tbl_df() %>%
    rename(lat = V1, long = V2)
  
  # plan(multisession)
  # ff_path <- paste0(tempfile(pattern = 'chirps_values'), '.ffdata')
  
  f <- future({
    
    vx_raster$extract(points, fun = mean) %>%
      tbl_df() %>%
      rename(precip = V1) %>%
      dplyr::bind_cols(coords) %>%
      dplyr::select(lat, long, precip) %>%
      as.ffdf.data.frame()
    # data.table
  }
  )
  
  # v <- value(f)  
  return(f)
  
  
  
}



extract_velox <- function(file, points){
  
  # r <- velox('/mnt/workspace_cluster_9/AgMetGaps/Chirps/daily//chirps-v2.0.1981.01.01.tif')
  # points <- plant_points
  
  vx_raster <- velox(file)
  
  
  coords <- coordinates(points) %>%
    tbl_df() %>%
    rename(lat = V1, long = V2)
  
  date_raster <- extract_date(file) %>%
    rep(dim(coords)[1])
  date_raster <- data_frame(date_raster)
  # plan(multisession)
  # ff_path <- paste0(tempfile(pattern = 'chirps_values'), '.ffdata')
  #   
  values <- vx_raster$extract(points, fun = mean) %>%
    tbl_df() %>%
    rename(precip = V1) %>%
    dplyr::bind_cols(coords, date_raster) %>%
    dplyr::select(lat, long, precip, date)
  # data.table
  
  
  return(values)
  
  
  
}

sack_to_points <- function(x){
  
  # x <- plant_start
  exchange <- names(x)
  variable <- stringr::str_replace_all(names(x), pattern = '[:punct:]+', replacement  = '_')
  x <- rasterToPoints(x) %>%
    tbl_df() %>%
    rename(!!variable := !!exchange, 
           lat = x, long = y)
  
  return(x)
}

make_date_information <- function(x){
  
  x %>%
    mutate(year = lubridate::year(date),
           month = lubridate::month(date),
           day = lubridate::day(date))
  
}

make_nest <- function(x){
  
  x %>%
    nest()
}

velox_lst <- function(x){
  
  
  y <- listenv::listenv()
  y <- future::future_lapply(x, FUN = velox) %>%
    velox::velox()
  
  
  
}





extract_velox <- function(file, points){
  
  # r <- velox('/mnt/workspace_cluster_9/AgMetGaps/Chirps/daily//chirps-v2.0.1981.01.01.tif')
  # points <- plant_points
  
  vx_raster <- velox(file)
  
  
  coords <- sp::coordinates(points) %>%
    tbl_df()
  # 
  date_raster <- extract_date(file) %>%
    rep(dim(coords)[1])
  date_raster <- data_frame(date_raster)
  # 
  # # plan(multisession)
  # # ff_path <- paste0(tempfile(pattern = 'chirps_values'), '.ffdata')
  # #
  values <- vx_raster$extract(points, fun = mean_point) %>%
    tbl_df() %>%
    # dplyr::rename(precip = V1) %>%
    dplyr::bind_cols(coords, date_raster) 
  # data.table
  
  
  # return(values)
  
  
  
}

velox_lst <- function(x, points_coord){
  
  y <- listenv::listenv()
  
  y <- foreach::foreach(j = 1:length(x)) %dopar% {
    
    extract_velox(file = x[j], points = points_coord)
  }
  # y <- future::future_lapply(x, FUN = extract_velox, points = points_coord)
  
  y
  
}

# make_joint <- function(x, y, ...){
#   
#   x <- x %>%
#     mutate(lat = as.character(lat), 
#            long = as.character(long))
#   y <- y %>%
#     mutate(lat = as.character(lat), 
#            long = as.character(long))
#   
#   left_join(x, y, ...)
# }  













library(sf)
x = st_read(dsn = paste0('//dapadfs/data_cluster_4/admin_boundaries/adminFiles/gadm_v1_lev0_shp/', 'gadm1_lev0.shp'))

library(velox)
library(raster)


vx2 <- velox(x)

vx2$as.RasterLayer()

sp_pdate <- rasterToPolygons(pdate,  dissolve = F) 
sp_pdate <- rasterToPoints(pdate) %>%
  sample()


# y <- rasterToPoints(pdate, spatial = T)
y <- as(SpatialPixelsDataFrame(sp_pdate[,c('x',x ''y)], sp_pdate[, 'values'], tolerance=.00086), 
        "SpatialPolygonsDataFrame")
values <- vx2$extract(y, fun = mean)



coords <- coordinates(y) %>%
  tbl_df() %>%
  rename(x =V1, y = V2)

proof <- values %>%
  tbl_df() %>%
  rename(values = V1) %>%
  bind_cols(coords) 




points(proof$x, proof$y)

mat <- matrix(1:100, 10, 10)
extent <- c(0,1,0,1)
vx <- velox(mat, extent=extent, res=c(0.1,0.1), crs="+proj=longlat +datum=WGS84 +no_defs")

## Make SpatialPolygonsDataFrame
library(sp)
library(rgeos)
set.seed(0)
coords <- cbind(runif(10, extent[1], extent[2]), runif(10, extent[3], extent[4]))
sp <- SpatialPoints(coords)
spol <- gBuffer(sp, width=0.2, byid=TRUE)
spdf <- SpatialPolygonsDataFrame(spol, data.frame(id=1:length(spol)), FALSE)

## Extract values and calculate mean
ex.mat <- vx$extract(spdf, fun=mean)



## Make VeloxRaster with two bands
set.seed(0)
mat1 <- matrix(rnorm(100), 10, 10)
mat2 <- matrix(rnorm(100), 10, 10)
vx <- velox(list(mat1, mat2), extent=c(0,1,0,1), res=c(0.1,0.1),
            crs="+proj=longlat +datum=WGS84 +no_defs")
## Make SpatialPoints
library(sp)
library(rgeos)
coord <- cbind(runif(10), runif(10))
spoint <- SpatialPoints(coords=coord)
## Extract
vx$extract_points(sp=spoint)


library("future")
library("listenv")

## Set up access to remote login node
login <- tweak(remote, workers = "climate.ciat.cgiar.org")
plan(login)

## Set up cluster nodes on login node
nodes %<-% { .keepme <- parallel::makeCluster(c("n1", "n2", "n3")) }

## Specify future topology
## login node -> { cluster nodes } -> { multiple cores }
plan(list(
  login,
  tweak(cluster, workers = nodes),
  multiprocess
))



## weather analysis

library(raster)
library(velox)
library(stringr)
library(dplyr)
library(ncdf4)
library(purrr)
library(future)
library(tidyr)
library(lubridate)
library(tidyverse)
library(purrrlyr)
library(lubridate)
library(iterators)
library(data.table)
library(ff)
library(listenv)
library(magrittr)

# chirps_path <- '/mnt/workspace_cluster_9/AgMetGaps/Chirps/daily/'
chirps_path <- '/mnt/data_cluster_4/observed/gridded_products/chirps/daily/'

# library("doFuture")
# registerDoFuture()  ## tells foreach futures should be used
# plan(multisession)

## cargar raster por dia?
## Generar codigo tanto para extraer .tif como .nc
## tif solo puede contener un atributo, archivos .nc puede contener varios atributos

# plan(list(tweak(cluster, workers = workers[1:20]), tweak(cluster, workers = workers[20:30])))
# plan(multiprocess, workers = availableCores() - 2)


raster_files <- list.files(chirps_path, pattern = '.tif$', full.names = T) %>%
  data_frame(file = .) %>%
  mutate(date = purrr::map(.x = file, .f = extract_date)) %>%
  tidyr::unnest() %>%
  mutate(year = lubridate::year(date),
         month = lubridate::month(date),
         day = lubridate::day(date))

## hacer filtro solo hasta 2016 (o hasta donde se requiera)

raster_files <- raster_files %>%
  filter(year <= 1983) 


nodes <- rep(c("localhost", "caribe.ciat.cgiar.org"), each = 4)
plan(list(tweak(cluster, workers = nodes), multiprocess))

m <- listenv()
for (ii in 1:100) {
  m[[ii]] %<-% {
    y <- listenv()
    for (jj in 1:100) {
      y[[jj]] %<-% { ii + jj / 10 }
    }
    as.list(y)
  }
}


# x <- raster_files %>%
#   nest(-year) %>%
#   mutate(vx_raster = purrr::map(data, .f = ~future(velox_year(.x))))

x <- raster_files %>%
  filter(year <= 1981) %>%
  nest(-month) %>%
  filter(row_number() <=4) %>%
  # mutate(vx_raster = purrr::map(data, .f = velox_year))
  mutate(vx_raster = purrr::map(data, .f = ~future(velox_year(.x))))

x <- x %>%
  mutate(vx_raster = purrr::map(.x = vx_raster, .f = ~value(.x)))




# plan(multisession, workers = availableCores() - 3)  ## para trabajar en una sola maquina
local_cpu <- rep("localhost", availableCores() - 3)
# external_cpu <- rep("climate.ciat.cgiar.org", 10)
external_cpu <- rep( c("climate.ciat.cgiar.org", "caribe.ciat.cgiar.org"), each = 8)

workers <- c(local_cpu, external_cpu)
plan(cluster, workers = workers)

library("doFuture")
registerDoFuture()  ## tells foreach futures should be used
plan(multisession) 


# system.time(
chirps_df <- raster_files %>%
  filter(row_number()<=30) %>%
  mutate(velox_raster = map(.x = file, .f = ~future(velox(.x)))) %>%
  mutate(velox_raster = map(velox_raster, ~value(.x)))

group_by(day) %>%
  do(., d = foreach(row = iter(., by = "row")) %dopar% {
    
    velox::velox(.$file)
  }
  
  
  
  
  )

mutate(velox_raster = map(.x = file, .f = ~future(velox(.x)))) %>%
  mutate(velox_raster = map(velox_raster, ~value(.x)))
# )
do(df.h, 
   .f=function(data){
     lm(price ~ wind + temp, data=data)
   })
foreach(row = iter(data, by="row"), .combine=c)
foreach(C = Cs) %dopar% {
  JuliaImage(1000, centre = 0 + 0i, L = 3.5, C = C)
}

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



raster_files <- list.files(chirps_path, pattern = '.tif$', full.names = T) %>%
  data_frame(file = .) %>%
  mutate(date = map(.x = file, .f = extract_date)) %>%
  # mutate(date = map(.x = date, ~value(.x))) %>%
  tidyr::unnest() %>%
  mutate(year = lubridate::year(date)) 




# filter(row_number() <= 100) %>%
# purrrlyr::invoke_rows(.f = extract_date, .to = 'date') %>%
# unnest()

split(.$file)
# purrrlyr::by_row(..f = extract_date)
group_by(file) %>%
  # split(.$file) %>%
  do(
    r = foreach(i = .) %dopar% {
      extract_date(filename = .$file)
      
    }
    
  )

do(f = future(extract_date(.$file)))


data2 <- split(data, data$Month)
test2 <- foreach(i = data2, .combine = rbind) %dopar% (data.frame(Month = unique(i$Month), Distance= mean(i$Distance)))
do(f= extract_date(.$file))


prueba <- function(chirps_file, start_plant, end_plant, points_coord){
  
  # chirps_file <- list.files(chirps_path, pattern = '.tif$', full.names = T)  ## chirps file
  # chirps_extent <- c(-180,  180,  -50,   50)  ## extent with where is the information
  # start_plant <- raster(paste0('/mnt/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/Maize.crop.calendar.nc'), varname = 'plant.start')
  # end_plant <- raster(paste0('/mnt/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/Maize.crop.calendar.nc'), varname = 'plant.end')
  
  
  
  raster_files <- chirps_file %>%
    data_frame(file = .) %>%
    mutate(date = purrr::map(.x = file, .f = extract_date)) %>%
    tidyr::unnest() %>%
    mutate(year = lubridate::year(date),
           month = lubridate::month(date),
           day = lubridate::day(date))
  
  # temp_raster <- rasterTmpFile()
  # extent_raster <- velox(chirps_file[1])
  
  ## este points_coord puede cambiar en el futuro 
  # points_coord <- start_plant %>%
  #   crop(chirps_extent) %>%
  #   writeRaster(filename = temp_raster, overwrite = TRUE) %>%
  #   rasterToPolygons(dissolve = F) 
  # 
  # rm(temp_raster)
  
  x <- raster_files %>%
    filter(year <= 1982) %>%
    pull(file)
  
  x <- x %>%
    base::split(.$year, drop = TRUE) %>%
    purrr::map(.f = filter, month == 1) %>%
    purrr::map(.f = pull, file)
  
  local_cpu <- rep("localhost", availableCores() - 3)
  # external_cpu <- rep("climate.ciat.cgiar.org", 10)
  external_cpu <- rep("climate.ciat.cgiar.org", each = 2)
  
  workers <- c(local_cpu, external_cpu)
  
  plan(list(tweak(cluster, workers = workers),multicore))
  
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
  prueba <- purrr::map(.x = m, .f = bind_rows)
  purrr::map(.x = prueba, .f = function(x) {x %>% mutate(year = lubridate::year(date_raster))}) %>%
    purrr::map(.f = function(x) x %>% group_by(V11, V2) %>% tidyr::nest())
  
  purrr::map(.x = , .f = function(x) x %>% mutate(year = lubridate::year(year)))
  prueba <- bind_rows(purrr::map(.x = m, .f = bind_rows))
  
  prueba %>%
    # purrr::map( .f = function(x) mutate(x, id = 1:nrow(x))) %>%
    bind_rows() %>%
    mutate(year = lubridate::year(date_raster)) %>%
    data.table::as.data.table() %>%
    split(., list(.$V11, .$V2))
  mutate(year = lubridate::year(date_raster)) %>%
    group_by(V11, V2) %>%
    tidyr::nest()
  # velox_lst()
  
  magrittr::extract2(2)
  
  tic("extract")
  y1 <- velox(list(velox(x[1]), velox(x[2]), velox(x[3]), velox(x[4])))
  
  y <- listenv()
  for(i in 1:length(points_coord)){
    
    y[[i]] %<-% {y1$extract( points_coord[i,], fun = mean_point)
    }
    # values(y)
    
    toc()
    
    y1$extract( points_coord[25214,], fun = function(x) {
      x[x<0] <- NA
      x <- as.numeric(x)
      
      mean(x, na.rm = T)})
    
    y <- stack(x[1:50])
    # plan(multisession)
    # plan(multiprocess, workers = 19)
    library(doFuture)
    library(tictoc)
    registerDoFuture() 
    # plan(multiprocess)
    
    # nodes <- rep(c("localhost", "caribe.ciat.cgiar.org", "climate.ciat.cgiar.org"), each = 4)
    # plan(cluster, workers = nodes)
    # plan(list(tweak(cluster, workers = nodes), tweak(multicore, workers = 4)))
    
    
    local_cpu <- rep("localhost", availableCores() - 3)
    # external_cpu <- rep("climate.ciat.cgiar.org", 10)
    external_cpu <- rep( c("climate.ciat.cgiar.org", "caribe.ciat.cgiar.org"), each = 8)
    nodes <- c(local_cpu, external_cpu)
    plan(list(tweak(cluster, workers = nodes), multiprocess))
    
    tic("load extract")
    chirps_df <- foreach(i = 1:length(chirps_file)) %dopar% {
      
      # i = 2
      extract_velox(file = chirps_file[i], points = points_coord)
      
    }
    toc()
    
    planting <- sack_to_points(start_plant %>% crop(chirps_extent))
    ending <- sack_to_points(end_plant %>% crop(chirps_extent))
    
    # prueba <- purrr::map(.x = chirps_df, .f = bind_cols, planting %>% dplyr::select(3), ending %>% dplyr::select(3)) 
    # prueba <- list(chirps_df[[1]], chirps_df[[2]], chirps_df[[3]])
    prueba <- purrr::map(.x = chirps_df, .f = make_date_information)
    prueba <- prueba %>%
      bind_rows()
    
    prueba <- prueba %>%
      filter(year <= 1983)
    
    
    
    ## nest in parallel
    # format(object.size(prueba), units = 'Gb')
    options(future.globals.maxSize = 2435686800)
    x <- prueba %>%
      group_by(year) %>%
      # split(.$file) %>%
      # do(head(.))
      do(
        
        
        make_nest(.)
        
        
        
      )
    
    x 
    
    # resolved(values) para saber si ya se resolvio el proceso
    
    # format(object.size(values %>% as.ffdf.data.frame()), units = 'Mb')
    rm(date); gc()
  }
  
  
  library("future")
  library("listenv")
  
  registerDoFuture()
  
  plan(list(multiprocess, sequential))
  x <- listenv()
  x[[ii]] <- foreach(ii  = 1:3) %dopar% {
    y <- listenv()
    y[[jj]] <- foreach (jj = 1:3) %do% {
      ii + jj / 10 
      
    }
    
  }
  
  mtcars %>%
    slice_rows("cyl") %>%
    by_slice(partial(lm, mpg ~ disp))
  mtcars %>% slice_rows(c("cyl", "am")) %>% by_slice(dmap, ~ .x / sum(.x), .collate = "list")
  
  x <- raster_files %>%
    nest(-year)
  purrr::map(x, magrittr::extract2, 1)$data
  magrittr::extract2()  
  
  
  
  library("future")
  library("listenv")
  m <- listenv()
  for (ii in 1:3) {
    m[[ii]] %<-% {
      y <- listenv()
      for (jj in 1:3) {
        y[[jj]] %<-% { ii + jj / 10 }
      }
      y
    }
  }
  
  library(data.table)
  
  dt = data.table(x1 = rep(letters[1:2], 6),
                  x2 = rep(letters[3:5], 4),
                  x3 = rep(letters[5:8], 3),
                  y = rnorm(12))
  dt = dt[sample(.N)]
  df = as.data.frame(dt)
  # split consistency with data.frame: `x, f, drop`
  all.equal(
    split(dt, list(dt$x1, dt$x2)),
    lapply(split(df, list(df$x1, df$x2)), setDT)
    
    
  