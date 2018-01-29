##################################################################
#######   Making crop calendar analysis (floration)       ######## 
##################################################################


# Libraries

library(raster)
library(ncdf4)
library(velox)
library(foreach)
library(future) # parallel package
library(doFuture)
library(magrittr)
library(lubridate)
library(tidyr)
library(readr)
library(stringr)
library(tidyverse)
library(chron)
library(geoR)


#####


# setwd('/mnt/workspace_cluster_9/AgMetGaps')

path <- "/mnt/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/"
crop <- 'rice' # rice , maize , wheat
crop_type <- 'Rice.crop.calendar.nc' # rice , maize , wheat

## Proof for rice information 


##################################################################
#######                   Planting dates                  ######## 
##################################################################


## Proof for maize information 

# D:/Agpurrr::maps/planting_dates/calendar/


pdate <- raster(paste0(path, crop_type), varname = 'plant') %>% 
  crop(extent(-180, 180, -50, 50)) 


pdate_points <- rasterToPoints(pdate) %>%
  tbl_df() %>% 
  setNames(nm = c('x', 'y', 'julian.start')) %>%
  mutate(Planting =  month.day.year(julian.start)$month)


hdate <- raster(paste0(path, crop_type), varname = 'harvest') %>% 
  crop(extent(-180, 180, -50, 50)) 

hdate_points <- rasterToPoints(hdate) %>%
  tbl_df() %>% 
  setNames(nm = c('x', 'y', 'julian.h')) %>%
  mutate(harvest =  month.day.year(julian.h)$month) 



# Compute floration trimester
compute_flor <- function(.x, .y){
  
  if(.x < .y){
    flor <- round(.x + (.y - .x)/2 , 0)
  } else if(.x > .y){
    flor <- round(.x +  ( (12 - .x) + .y)/2 , 0)
  }
  
  if(flor> 12){flor <- flor - 12} else if( flor <= 12){flor <- flor}
  
  return(flor)
  }




month_below <- function(.x){
  if(.x > 1) { below <- .x - 1} else if(.x  == 1){ below <- 12 }
  return(below)}



month_above <- function(.x){
  if(.x < 12) { above <- .x + 1} else if(.x  == 12){ above <- 1 }
  return(above)}




crop.time <- inner_join(pdate_points, hdate_points) %>% 
  select(-julian.start, -julian.h) %>% 
  mutate(flor = purrr::map2(.x = Planting, .y = harvest, .f = compute_flor)) %>% 
  unnest() %>% 
  select(x, y, Planting, flor, harvest) %>% 
  mutate(a.Planting1 = purrr::map(.x = Planting, .f = month_below), 
         a.Planting3 = purrr::map(.x = Planting, .f = month_above), 
         b.flor1 = purrr::map(.x = flor, .f = month_below), 
         b.flor3 = purrr::map(.x = flor, .f = month_above),
         c.harvest1= purrr::map(.x = harvest, .f = month_below), 
         c.harvest3 = purrr::map(.x = harvest, .f = month_above)) %>% 
  unnest  %>% 
  rename(a.Planting2 = Planting, b.flor2 = flor, c.harvest2 = harvest) %>% 
  select(x,y, a.Planting1, a.Planting2, a.Planting3, b.flor1, b.flor2, b.flor3, 
         c.harvest1, c.harvest2, c.harvest3) %>% 
  gather(key = phase.month, value = month, -x, -y)  %>% 
  # group_by(x, y)  %>% 
  separate(phase.month, c("phase", "type"), sep = "\\.") 



##################################################################
##########      Functions to perform read raster,       ##########
##########    convert to velox objects, extract points, ##########
##########      convert to raster if it is necessary.   ########## 
##################################################################


###### function to read modificate raster to change band .y

raster_mod <- function(.x, .y){
  raster(.x, band = .y)
}



#### Create a function to strack the points 
extract_velox <- function(velox_Object, points){
  
  coords <- coordinates(points) %>%
    tbl_df() %>%
    rename(lat = V1, long = V2)
  
  velox_Object$extract(points, fun = function(x){ 
    x[x<0] <- NA
    x <- as.numeric(x)
    mean(x, na.rm = T)}) %>%
    tbl_df() %>%
    rename(values = V1) %>%
    bind_cols(coords) %>%
    dplyr::select(lat, long, values)
  
}



##############################
##############################
# To use the spatial polygons information in the velox strack to points 

sp_pdate <- rasterToPolygons(pdate,  dissolve = F) 


##################################################################
##################################################################
##########        Climate:  variability  analysis       ########## 
##################################################################
##################################################################



##################################################################
##########        Precipitation: Chirps                 ########## 
##################################################################

# 'D:/Agpurrr::maps/Chips_Monthly/'
## asignar la banda a que Año y mes pertenecen
n_bands <- nc_open(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'))$dim$time$len
n_bands <- n_bands - 8 # temporal




## read monthly precipitation rasters with furure functions 
plan(multisession, workers = availableCores() - 3)

system.time(  
  ## read monthly precipitation rasters 
  dates_raster <- "1981-1-1"  %>% # str_extract(nc_open(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'))$dim$time$units, "\\d{4}-\\d{1}-\\d{1}")
    as.Date() %>%
    seq(by = "month", length.out = n_bands) %>%
    data_frame(date = .) %>%
    mutate(year = year(date), month = month(date)) %>%
    mutate(band = 1:n_bands, file = rep(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'), n_bands)) %>%
    mutate(load_raster = purrr::map2(.x = file, .y = band, .f = ~future(raster_mod(.x,.y)))) %>%
    mutate(load_raster = purrr::map(.x = load_raster, .f = ~value(.x))) %>% 
    mutate(raster_df = purrr::map(.x = load_raster, .f = ~future(velox(.x)))) %>%
    mutate(raster_df = purrr::map(raster_df, .f = ~value(.x))) 
)
  
  


#### Extract points

# plan(multisession, workers = availableCores() - 4)

system.time(  
  dates_raster <- dates_raster %>% 
  mutate(points = purrr::map(.x = raster_df, .f = ~future(extract_velox(velox_Object = .x, points = sp_pdate)))) %>%
  mutate(points = purrr::map(points, ~value(.x))) 
)






##### Understand the extract information for each pixel 

# This part extact only the information for year, 
# month and important points


point_dates <-  dates_raster %>% 
  select( year, month, points) %>% 
  unnest %>% 
  group_by(lat, long) %>%
  nest() %>%
  mutate(id = 1:nrow(.))



# information for planting 
planting <- crop.time %>% 
  filter(phase == 'a') %>% 
  rename(lat = x,  long =  y) %>%
  group_by(type) %>%
  mutate(id = 1:length(type)) %>%
  ungroup()


### This function extract for each pixel the chirps information (precipitation)
### i represents the exactly pixel 

make_station <- function(x, file_name){
  
  weather_station <- x %>% 
    bind_rows() %>%
    unnest()
  
  coord <- weather_station %>%
    dplyr::select(lat, long) %>%
    distinct()
  
  lat <- dplyr::pull(coord, 'lat')
  long <- dplyr::pull(coord, 'long')
  
  
  write_csv(weather_station, path = paste0(paste(file_name, lat, long, sep = '_'), '.csv'))
  return(weather_station)}

point_extract <- function(x, y){
  
  proof <-  x  %>%
    inner_join(y , ., by = c('id', 'month')) %>%
    dplyr::select(-lat.y, -long.y) %>%
    group_by(phase, year, lat.x, long.x) %>%
    rename(lat = lat.x, long = long.x, precip =  values) %>% 
    summarise(prec_clim = sum(precip)) %>%
    ungroup()
  
  return(proof)
  
  }

extract_months <- function(dates, atelier, out1, name){
  
  folder <- atelier %>% 
    dplyr::select(phase) %>% 
    filter(row_number() == 1) %>% 
    as.character
  
  
  out <- paste0(out1, folder, '/')
  if(dir.exists(out) ==  'FALSE'){dir.create(paste0(out1, folder, '/'))}else if(dir.exists(out) ==  'TRUE'){print('TRUE')}
  file_name <- paste0( out, name )
  
  
  ### Is necessary parallelize the process?
  test <-point_dates %>% 
    mutate(i = 1:nrow(.)) %>%
    nest(-i) %>% 
    mutate(stations = purrr::map(.x =  data , .f =  make_station, file_name = file_name)) %>% 
    mutate(each_Pclim =  purrr::map(.x =  stations, .f = point_extract, y = atelier))
  
  return(test) } # in case necesary filter for 100 pixels 




calendar <- crop.time 

out1 <- paste0('/mnt/workspace_cluster_9/AgMetGaps/monthly_out/', crop,'/precip/')
name <- paste0(crop, '_precip')
 



#system.time(test <-   extract_months(dates = point_dates, atelier = planting, out = out1, name = name))

id_creation <- function(.x){
  .x %>% 
    group_by(type) %>%
    mutate(id = 1:length(type)) %>%
    ungroup() 
}


system.time(
  idea_precipitation <- crop.time %>% 
    rename(long = x,  lat  =  y) %>%
    mutate(control =  phase)  %>% 
    nest(-control) %>% 
    mutate(id_data = purrr::map(.x = data, .f = id_creation)) %>% 
    select(control, id_data) %>% 
    mutate(ext.months = purrr::map(.x = id_data,  .f = extract_months, dates = point_dates, 
                                   out = out1, name = name ) )
)

# user   system  elapsed 
# 3308.409   71.275 3903.678 





###### No borrar nunca el vector idea... pues contiene la informacion climatica 
##### entonces con el se pueden realizar otro tipo de calculos



climatology <- function(test, out1){
  
  # creation to the climatology
  prec_chirps <- test %>%  
    dplyr::select(i, each_Pclim) %>% 
    unnest %>%
    group_by(i, lat,  long, phase) %>%
    summarise(climatology_prec = mean(prec_clim), sd.prec = sd(prec_clim)) %>%
    mutate(cv.prec = (sd.prec/climatology_prec) * 100) %>% 
    ungroup
  
  
  file <- prec_chirps %>% select(phase) %>% .[1, ] %>% as.character
  write.csv(x = prec_chirps, file = paste0(out1, 'Climatology_', file, '.csv'), row.names = FALSE)
  
  return(prec_chirps)}




raining <- idea_precipitation %>%
  mutate(clima =  purrr::map(.x = ext.months, .f = climatology, out1 = out1)) %>%
  select(control, clima)




# for default rasterize with 'pdate'. 
# add out_path parameter to save the information


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 


out_path <- '/mnt/workspace_cluster_9/AgMetGaps/monthly_out/'



rasterize_mod <- function(x, raster, var){
  
  points <- x %>%
    select(long, lat) %>%
    data.frame 
  
  vals <- x %>%
    dplyr::select(!!var) %>%
    magrittr::extract2(1)
  
  y <- rasterize(points, raster, vals, fun = sum)
  
  ### Lectura del shp
  shp <- shapefile(paste("/mnt/workspace_cluster_9/AgMetGaps/Inputs/shp/mapa_mundi.shp",sep="")) %>% 
    crop(extent(-180, 180, -50, 50))
  
  u <- borders(shp, colour="black")
  
  
  myPalette <-  colorRampPalette(c("navyblue","#2166AC", "dodgerblue3","lightblue", "lightcyan",  "white",  "yellow","orange", "orangered","#B2182B", "red4"))
  
  ewbrks <- c(seq(-180,0,45), seq(0, 180, 45))
  nsbrks <- seq(-50,50,25)
  ewlbls <- unlist(lapply(ewbrks, function(x) ifelse(x < 0, paste(abs(x), "°W"), ifelse(x > 0, paste( abs(x), "°E"),x))))
  nslbls <- unlist(lapply(nsbrks, function(x) ifelse(x < 0, paste(abs(x), "°S"), ifelse(x > 0, paste(abs(x), "°N"),x))))
  
  # Blues<-colorRampPalette(c('#fff7fb','#ece7f2' ,'#edf8b1','#9ecae1', '#7fcdbb','#2c7fb8','#a6bddb','#1c9099','#addd8e', '#31a354'))
  Blues<-colorRampPalette(c('#fff7fb','#ece7f2','#d0d1e6','#a6bddb','#74a9cf','#3690c0','#0570b0','#045a8d','#023858','#233159'))
  
  
  rasterVis::gplot(y) + 
    geom_tile(aes(fill = value)) + 
    u + 
    coord_equal() +
    labs(x="Longitude",y="Latitude", fill = " ")   +
    scale_fill_gradientn(colours =Blues(100), na.value="white") +
    scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
    scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
    theme_bw() + theme(panel.background=element_rect(fill="white",colour="black")) 
  
  
  temp <- x %>% select(phase) %>% .[1,] %>% as.character
  file_name <- paste0(out_path , crop, '/precip/graphs_rasters/', temp, '.', var)
  ggsave(paste0( file_name, '.png'), width = 10, height = 4)
  writeRaster(y,  paste0( file_name, '.tif'))
  
  return(y)}


rasters_raining <- raining %>% 
  mutate(mean = purrr::map(.x =  clima, .f =  rasterize_mod, raster = pdate, var = 'climatology_prec')) %>%
  mutate(sd = purrr::map(.x =  clima, .f =  rasterize_mod, raster = pdate, var = 'sd.prec')) %>% 
  mutate(cv = purrr::map(.x =  clima, .f =  rasterize_mod, raster = pdate, var = 'cv.prec'))









##################################################################
##########     Mean Temperature: NCEP CPC GHCN_CAMS     ########## 
##################################################################


# read a one raster to GHCN_CAMS


raster_mod_temp <- function(.x, .y){
    raster(.x, band = .y) %>% 
    rotate
 }



  extract_velox_temp <- function(velox_Object, points){
      coords <- coordinates(points) %>%
        tbl_df() %>%
        rename(lat = V1, long = V2)
      velox_Object$extract(points, fun = function(x){ 
        mean(x, na.rm = T)}) %>%
        tbl_df() %>%
        rename(values = V1) %>%
        bind_cols(coords) %>%
        dplyr::select(lat, long, values)
    }

  
  

## asignar la banda a que Año y mes pertenecen
n_bands <- nc_open(paste0('Inputs/GHCN_CAMS/', 'data.nc'))$dim$T$len
n_bands <- 36*12 # temporal 1981 to 2016
  

# with future functions 
# plan(multisession, workers = availableCores() - 3)

system.time(  
  # read monthly temperature
GHCN_CAMS <-   str_extract(nc_open(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'))$dim$time$units,
              "\\d{4}-\\d{1}-\\d{1}") %>%
  as.Date() %>%
  seq(by = "month", length.out = n_bands)  %>%
  data_frame(date = .) %>%
  mutate(year = year(date), month = month(date)) %>%
  mutate(band = 1:n_bands, file = rep(paste0('Inputs/GHCN_CAMS/', 'data.nc'), n_bands)) %>%
  mutate(load_raster = purrr::map2(.x = file, .y = band, .f = ~future(raster_mod_temp(.x,.y)))) %>%
  mutate(load_raster = purrr::map(.x = load_raster, .f = ~value(.x))) %>% 
  mutate(raster_df = purrr::map(.x = load_raster, .f = ~future(velox(.x)))) %>%
  mutate(raster_df = purrr::map(raster_df, .f = ~value(.x))) %>% 
  mutate(points = purrr::map(.x = raster_df, .f = ~future(extract_velox_temp(velox_Object = .x, points = sp_pdate)))) %>%
  mutate(points = purrr::map(points, ~value(.x))) 
)




###### Creation for the temperatures seasonals and climatologies 

point_temp <-  GHCN_CAMS %>% 
  select( year, month, points) %>% 
  unnest %>% 
  mutate(valuesC = values - 273.15) %>%
  group_by(lat, long) %>%
  nest() %>%
  mutate(id = 1:nrow(.))



# point_temp  %>% unnest %>% mutate(valuesC= values - 273.15) %>%  select(values, valuesC) %>% summary()


### This function extract for each pixel temp information 
### i represents the exactly pixel 



### This function extract for each pixel temp information 
### i represents the exactly pixel 


# Min.   :233.8  
# 1st Qu.:288.1  
# Median :296.3  
# Mean   :293.0  
# 3rd Qu.:300.0  
# Max.   :335.8  
# NA's   :1728 



point_extractT <- function(x, y){
  
  proof <-  x  %>%
    inner_join(y , ., by = c('id', 'month')) %>%
    dplyr::select(-lat.y, -long.y) %>%
    group_by(phase, year, lat.x, long.x) %>%
    rename(lat = lat.x, long = long.x, tempk =  values, tempC = valuesC) %>% 
    summarise(Temp_clim = mean(tempC), Temp_k = mean(tempk)) %>%
    ungroup()
  
  return(proof)}




extract_monthsT <- function( dates, atelier, out1, name){
  
  folder <- atelier %>% 
    select(phase) %>% 
    filter(row_number() == 1) %>% 
    as.character
  
  
  out <- paste0(out1, folder, '/')
  if(dir.exists(out) ==  'FALSE'){dir.create(paste0(out1, folder, '/'))}else if(dir.exists(out) ==  'TRUE'){print('TRUE')}
  
  file_name <- paste0( out, name2 )
  
  ### Is it necessary parallelize the process?
  test <- dates %>% 
    mutate(i = 1:nrow(.)) %>%
    nest(-i) %>% 
    mutate(stations = purrr::map(.x =  data , .f =  make_station, file_name =  file_name))  %>%   
    mutate(each_Pclim =  purrr::map(.x =  stations, .f = point_extractT, y = atelier))
  
  return(test) } # in case necesary filter for 100 pixels 




############


out2 <-  '/mnt/workspace_cluster_9/AgMetGaps/monthly_out/rice/temp/'
name2 <- 'rice_temp'




system.time(
  idea_temp <- crop.time %>% 
    rename(long = x,  lat  =  y) %>%
    mutate(control =  phase)  %>% 
    nest(-control) %>% 
    mutate(id_data = purrr::map(.x = data, .f = id_creation)) %>% 
    select(control, id_data) %>% 
    mutate(ext.months = purrr::map(.x = id_data,  .f = extract_monthsT, dates = point_temp   , 
                                   out = out2, name = name2 ) )
)

# user   system  elapsed 
# 3864.637  132.726 4506.330 


# raster(paste0('Inputs/GHCN_CAMS/', 'data.nc')) %>% crop(extent( 0, 360, -50, 50 )) %>%  rotate %>% plot




climatologyT <- function(test, out1){
  
  # creation to the climatology
  T.GHCN_CAMS <- test %>%  
    select(i, each_Pclim) %>% 
    unnest %>%
    group_by(i, lat,  long, phase) %>%
    summarise(climatology_temp = mean(Temp_k), sd.temp = sd(Temp_k)) %>%
    mutate(cv.temp = (sd.temp/climatology_temp) * 100) %>% 
    ungroup
  
  
  file <- T.GHCN_CAMS %>% select(phase) %>% .[1, ] %>% as.character
  write.csv(x = T.GHCN_CAMS, file = paste0(out1, 'TClimatology_', file, '.csv'), row.names = FALSE)
  
  return(T.GHCN_CAMS)}



idea_temp %>%
  filter(row_number() < 2) %>% 
  dplyr::select(ext.months) %>%
  unnest %>%
  dplyr::select(i, each_Pclim) %>% 
  unnest 





temperature <- idea_temp %>%
  mutate(climaT =  purrr::map(.x = ext.months, .f = climatologyT, out1 = out2)) %>%
  dplyr::select(control, climaT)






# for default rasterize with 'pdate'. 
rasterize_modT <- function(x, raster, var){
  
  points <- x %>%
    dplyr::select(long, lat) %>%
    data.frame 
  
  vals <- x %>%
    dplyr::select(!!var) %>%
    magrittr::extract2(1)
  
  y <- rasterize(points, raster, vals, fun = sum)
  
  ### Lectura del shp
  shp <- shapefile(paste("/mnt/workspace_cluster_9/AgMetGaps/Inputs/shp/mapa_mundi.shp",sep="")) %>% 
    crop(extent(-180, 180, -50, 50))
  u<-borders(shp, colour="black")
  

  ewbrks <- c(seq(-180,0,45), seq(0, 180, 45))
  nsbrks <- seq(-50,50,25)
  ewlbls <- unlist(lapply(ewbrks, function(x) ifelse(x < 0, paste(abs(x), "°W"), ifelse(x > 0, paste( abs(x), "°E"),x))))
  nslbls <- unlist(lapply(nsbrks, function(x) ifelse(x < 0, paste(abs(x), "°S"), ifelse(x > 0, paste(abs(x), "°N"),x))))
  

  
  rasterVis::gplot(y) + 
    geom_tile(aes(fill = value)) + 
    u + 
    coord_equal() +
    labs(x="Longitude",y="Latitude", fill = " ")   +
    scale_fill_distiller(palette = "RdBu", na.value="white") +
    scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
    scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
    theme_bw() + theme(panel.background=element_rect(fill="white",colour="black")) 
  
  
  temp <- x %>% select(phase) %>% .[1,] %>% as.character
  file_name <- paste0('/mnt/workspace_cluster_9/AgMetGaps/monthly_out/', crop, '/temp/graphs_rasters/', temp, '.', var)
  ggsave(paste0( file_name, '.png'), width = 10, height = 4)
  writeRaster(y,  paste0( file_name, '.tif'))
  
  ### Write in GeoJeison format 
  
  return(y)}


rasters_temperature <- temperature %>% 
  mutate(mean = purrr::map(.x =  climaT, .f =  rasterize_modT, raster = pdate, var = 'climatology_temp')) %>%
  mutate(sd = purrr::map(.x =  climaT, .f =  rasterize_modT, raster = pdate, var = 'sd.temp')) %>% 
  mutate(cv = purrr::map(.x =  climaT, .f =  rasterize_modT, raster = pdate, var = 'cv.temp'))





# 38286 


##################################################################
##################################################################
##########                   Yield Gaps                 ########## 
##################################################################
##################################################################


Yield.G <- list.files(path = 'Inputs/Yield_Gaps_ClimateBins/rice_yieldgap_netcdf/', 
                      pattern = 'YieldGap.nc', full.names = TRUE) %>%
  as.tibble %>% 
  mutate(load_raster = purrr::map(.x = value, .f = function(.x){ 
    raster(.x) %>%  crop(extent(-180, 180, -50, 50))})) %>%
  mutate(raster_df =  purrr::map(.x = load_raster, .f = velox::velox) ) 




shp <- shapefile(paste("/mnt/workspace_cluster_9/AgMetGaps/Inputs/shp/mapa_mundi.shp",sep="")) %>% 
  crop(extent(-180, 180, -50, 50))

u <- borders(shp, colour="black")
Blues<-colorRampPalette(c('#fff7fb','#ece7f2','#d0d1e6','#a6bddb','#74a9cf','#3690c0','#0570b0','#045a8d','#023858','#233159'))
ewbrks <- c(seq(-180,0,45), seq(0, 180, 45))
nsbrks <- seq(-50,50,25)
ewlbls <- unlist(lapply(ewbrks, function(x) ifelse(x < 0, paste(abs(x), "°W"), ifelse(x > 0, paste( abs(x), "°E"),x))))
nslbls <- unlist(lapply(nsbrks, function(x) ifelse(x < 0, paste(abs(x), "°S"), ifelse(x > 0, paste(abs(x), "°N"),x))))





list.files(path = 'Inputs/Yield_Gaps_ClimateBins/rice_yieldgap_netcdf/', 
           pattern = 'YieldGap.nc', full.names = TRUE) %>% raster %>% 
  crop(extent(-180, 180, -50, 50))  %>% 
  rasterVis::gplot(.) + 
  geom_tile(aes(fill = value)) + 
  u + 
  coord_equal() +
  labs(x="Longitude",y="Latitude", fill = " ")   +
  scale_fill_gradientn(colours =Blues(100), na.value="white") +
  scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
  scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
  theme_bw() + theme(panel.background=element_rect(fill="white",colour="black")) 




coords <- coordinates(sp_pdate) %>%
  tbl_df() %>%
  rename(lat = V1, long = V2)



test_yield <- Yield.G$raster_df[[1]]$extract(sp_pdate, fun = function(x){ 
  mean(x, na.rm = T)}) %>%
  tbl_df() %>%
  rename(gap = V1) %>%
  bind_cols(coords) %>%
  mutate(i = 1:nrow(.)) %>%
  dplyr::select(i, lat, long, gap)


test_yield[which(test_yield$gap[] == 'NaN'), 4] <- NA
test_yield





##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################


# .x =  data    .y = control 
change_names <- function(.x, .y){
  name <- .y %>%
    as.character()
  
  all <- .x %>% 
    dplyr::select(-phase) %>%
    names
  
  final <- .x %>% 
    dplyr::select(-phase) %>%
    set_names(., paste0(name, '.', all))  %>% 
    rename(i = !!paste0(name, '.', all[1]), 
           lat = !!paste0(name, '.', all[2]), 
           long = !!paste0(name, '.', all[3]))
  return(final)}



I.Temp <- temperature %>% 
  mutate(clima =  purrr::map2(.x = climaT, .y = control, .f = change_names)) %>%
  dplyr::select(clima) %>% 
  split(., f = c('a', 'b', 'c')) %>%
  lapply(., unnest) %>%
  reduce(inner_join)


I.Prec <- raining %>% 
  mutate(climate =  purrr::map2(.x = clima, .y = control, .f = change_names)) %>%
  dplyr::select(climate)  %>% 
  split(., f = c('a', 'b', 'c')) %>%
  lapply(., unnest) %>%
  reduce(inner_join)




Climate <- inner_join(I.Prec, I.Temp)




OB1 <- inner_join(Climate, test_yield %>% select(-lat, -long), by = 'i')

OB1 %>% select(gap) %>% summary
# NA's   :11315  # Total = 23834   # Pixels coincidentes 12519 



COB1 <- OB1 %>% na.omit 
COB1 %>% write.csv(file = paste0('monthly_out/', crop, '/summary.csv'), row.names = FALSE)



##### In this parth we don't use anymore the climate information for that today, i want to clear memory. 


# clean memory 
gc(verbose = TRUE)
gc(reset = TRUE)
rm(list = ls())



setwd('/mnt/workspace_cluster_9/AgMetGaps')
crop <- 'rice' # rice , maize , wheat




######## variograms 


tbl <- read.csv(paste0('monthly_out/',crop,'/summary.csv')) # read database in case we don't have it in memory
View(tbl)

var <- names(tbl)[-(1:3)]



#### Creation function to make variograms for all variables, (revision of the spatial correlation).

var1 <- var[1]
all_variograms <- function(var, tbl){
  
  geo_gap <- tbl %>% dplyr::select(long, lat, !!var) %>% as.geodata()
  
  
  # binned variogram
  vario.b <- variog(geo_gap)
  env.mc <- variog.mc.env(geo_gap, obj.var=vario.b)
  
  png(file = paste0(out_path, crop, '/variograms/variogram_', var, '.png'),  width = 720,
      height = 600)
  plot(vario.b, envelope=env.mc, col = 'red', pch =  19, main = var, lwd =3)
  dev.off()
}

var %>% 
  as.list %>% 
  lapply(all_variograms, tbl = tbl)






##### reviews 





##################################################################
##################################################################
##########          Correlations and rasterize          ########## 
##################################################################
##################################################################

#### Correlations and rasterize ---

# if you clean af objects of the memory. 
path <- "/mnt/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/"
crop <- 'rice' # rice , maize , wheat
crop_type <- 'Rice.crop.calendar.nc' # rice , maize , wheat
pdate <- raster(paste0(path, crop_type), varname = 'plant') %>%  crop(extent(-180, 180, -50, 50))

pdate %>%
  plot


###### ----- 


rasterize_masa <-  function(var, data, raster){
  points <- data %>%
    select(long, lat) %>%
    data.frame 
  
  
  vals <- data %>%
    select(!!var) %>%
    magrittr::extract2(1)
  
  y <- rasterize(points, raster, vals, fun = sum)
  return(y)}



##### Read all data set how rasters
all_inf <- tbl %>%  
  names  %>% 
  .[-(1:3)] %>% 
  as.tibble() %>% 
  rename(var = value) %>% 
  mutate(rasters = purrr::map(.x = var, 
                              .f = rasterize_masa, 
                              data = tbl, raster = pdate))





##### create a raster stack object with the all variables to the data set 
all_inf <- tbl %>% 
  names %>%
  .[-(1:3)] %>%
  as.list() %>%
  lapply(., rasterize_masa,  data = tbl, raster = pdate) %>% 
  stack %>% 
  set_names(tbl %>%  
              names  %>% 
              .[-(1:3)])





# all_inf %>% crop(a) %>% getValues() %>%  summary()

# y = all_inf$gap
# x = cualquier otro raster 


correlations_exp <- function(.x, .y, data, ngb){
  
  variables <- stack( data[[which(data %>% names == .x )]], data[[which(data %>% names == .y )]])
  RI <- corLocal(variables[[1]], variables[[2]], ngb= ngb,   method = "pearson" )  
  
  ### Lectura del shp
  shp <- shapefile(paste("/mnt/workspace_cluster_9/AgMetGaps/Inputs/shp/mapa_mundi.shp",sep="")) %>% 
    crop(extent(-180, 180, -50, 50))
  u<-borders(shp, colour="black")
  
  ewbrks <- c(seq(-180,0,45), seq(0, 180, 45))
  nsbrks <- seq(-50,50,25)
  ewlbls <- unlist(lapply(ewbrks, function(x) ifelse(x < 0, paste(abs(x), "°W"), ifelse(x > 0, paste( abs(x), "°E"),x))))
  nslbls <- unlist(lapply(nsbrks, function(x) ifelse(x < 0, paste(abs(x), "°S"), ifelse(x > 0, paste(abs(x), "°N"),x))))
  
  
  
  RI %>% 
    rasterVis::gplot(.) + 
    geom_tile(aes(fill = value)) + 
    u + 
    coord_equal() +
    labs(x="Longitude",y="Latitude", fill = " ")   +
    scale_fill_distiller(palette = "RdBu", na.value="white") + 
    scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
    scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
    theme_bw() + theme(panel.background=element_rect(fill="white",colour="black")) 
  
  ggsave( paste0(out_path ,crop,'/correlations/', .x, '_', .y, '_ngb_',ngb, '.png'), width = 8, height = 3.5)
  writeRaster(x = RI, filename = paste0(out_path ,crop,'/correlations/', .x,'_', .y, '_ngb_',ngb,'.tif'))
  
  
  # changed the correlation umbral 
  (RI > 0.2) %>% 
    rasterVis::gplot(.) + 
    geom_tile(aes(fill = value)) + 
    u + 
    coord_equal() +
    labs(x="Longitude",y="Latitude", fill = " ")   +
    scale_fill_distiller(palette =  "Blues", na.value="white", direction = 1) + 
    scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
    scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
    theme_bw() + theme(panel.background=element_rect(fill="white",colour="black"), legend.position = 'none') 
  
  ggsave( paste0(out_path,crop,'/correlations/abs_', .x,'_', .y, '_ngb_',ngb, '.png'), width = 8, height = 3.5)
  
  RI.i <- length(which(abs(RI[]) > 0.2)) / length(na.omit(RI[])) * 100
  
  return(RI.i)}

df <- all_inf %>%
  names %>% 
  as.tibble() %>% 
  slice(-n()) %>% 
  mutate(pixelC =  purrr::map(.x = value, .f = correlations_exp ,.y = 'gap', data = all_inf))



### save summary file (which percent of pixels has correlation >0.5)
df %>% 
  unnest %>% 
  write.csv(file = paste0('monthly_out/',crop,'/correlations/Local_cor_ngb3.csv'))





##### Only for preliminar clusters only with the cv information

abs_C <- list.files(path = paste0('monthly_out/',crop,'/correlations'), pattern = '*.cv.*3.tif*', full.names = TRUE)  %>% 
  stack %>% 
  abs() 






abs_C1 <- (abs_C > 0.2) %>%
  sum 



abs_C1 %>% 
  rasterVis::gplot(.) + 
  geom_tile(aes(fill = value)) + 
  u + 
  coord_equal() +
  labs(x="Longitude",y="Latitude", fill = " ")   +
  scale_fill_distiller(palette =  "Spectral", na.value="white") + 
  scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
  scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
  theme_bw() + theme(panel.background=element_rect(fill="white",colour="black")) 


ggsave(paste0('monthly_out/',crop,'/correlations/cor_max_02.png'), width = 10, height = 4)




(abs_C1 == 6) %>% 
  rasterVis::gplot(.) + 
  geom_tile(aes(fill = value)) + 
  u + 
  coord_equal() +
  labs(x="Longitude",y="Latitude", fill = " ")   +
  scale_fill_distiller(palette =  "Spectral", na.value="white") + 
  scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
  scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
  theme_bw() + theme(panel.background=element_rect(fill="white",colour="black")) 


ggsave(paste0('monthly_out/',crop,'/correlations/cor_max_02_6var.png'), width = 10, height = 4)






#### only evaluate the cv correlation with the yield_gap

tbl %>% summary

# list.files(path = paste0('monthly_out/',crop,'/correlations'), pattern =  glob2rx('*cv*.tif*') , full.names = TRUE)
#  glob2rx('*cv*.tif*') == "^.*cv.*\\.tif"



#### Temporal function to modificate 

scatter_graphs <- function(.x, .y, proof){
  
  proof %>%  
    ggplot2::ggplot(., aes(x =  proof[,.x],y = proof[,.y])) + 
    geom_point(alpha=2/10, shape=21, fill="blue", colour="black", size=2) +
    geom_smooth(method = "lm", se = FALSE) +
    labs(x= .x, y = .y, title = 'Correlation',
         subtitle = paste0('Pearson = ', round(cor(proof[,.x], proof[,.y]),3), '    -   Spearman = ', round(cor(proof[,.x], proof[,.y], method = 'spearman'),3))) + 
    theme_bw() 
  
  ggsave(filename = paste0('monthly_out/rice/dispersion/',.x, '_', .y, '.png'))
}





tbl %>% 
  names %>% 
  .[-(1:3)] %>%
  as.tibble() %>% 
  slice(-n()) %>%
  mutate(test = purrr::map(.x = value, .f = scatter_graphs, .y = 'gap', proof = tbl))



tbl %>% 
  .[-(1:3)] %>%
  cor() %>% 
  write.csv(file = paste0('monthly_out/',crop,'/dispersion/corP.csv'))


tbl %>% 
  .[-(1:3)] %>%
  cor(method = 'spearman') %>% 
  write.csv(file = paste0('monthly_out/',crop,'/dispersion/corS.csv'))



tbl %>% 
  .[-(1:3)] %>%
  cor(method = 'kendall') %>% 
  write.csv(file = paste0('monthly_out/',crop,'/dispersion/cork.csv'))









###################################################################################
########## Test for ACP (explonatory analysis). Initialy is only for verification.  

library(FactoMineR)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# all others with gap 
tbl %>% 
  select( -i, -lat, -long, -a.cv.prec, -b.cv.prec, -c.cv.prec,
          -a.cv.temp, -b.cv.temp, -c.cv.temp) %>%
  PCA()



# cv's with gap 
tbl %>% 
  select(a.cv.prec,b.cv.prec, c.cv.prec, a.cv.temp,b.cv.temp,c.cv.temp, gap) %>% 
  PCA()





#### PCA cv for all times and variables 


res.pca <- tbl %>% 
  select(a.cv.prec,b.cv.prec, c.cv.prec, a.cv.temp,b.cv.temp,c.cv.temp) %>% 
  PCA()


## plot of the eigenvalues
barplot(res.pca$eig[,1],main="Eigenvalues",names.arg=1:nrow(res.pca$eig))
summary(res.pca)
dimdesc(res.pca, axes = 1:2)


res.pca %>% names





res.pca$ind$coord %>% 
  .[,1:2] %>% 
  cbind.data.frame(gap = tbl$gap) %>%
  cor 


res.pca$ind$coord %>% 
  .[,1:2] %>% 
  cbind.data.frame(gap = tbl$gap) %>%
  plot













#########################################################################################
#########################################################################################
#########################################################################################
##########################    GROC Analysis                         
#########################################################################################
#########################################################################################
#########################################################################################

list.files(paste0(out_path, crop, '/GROC'), pattern = '.tif', 
           full.names = TRUE) %>% 
  stack() %>% 
  plot




GROC <- list.files(paste0(out_path, crop, '/GROC'), pattern = '.tif', 
                   full.names = TRUE)  %>%
  as.tibble %>% 
  mutate(load_raster = purrr::map(.x = value, .f = function(.x){ 
    raster(.x) %>%  crop(extent(-180, 180, -50, 50))})) %>%
  mutate(raster_df =  purrr::map(.x = load_raster, .f = velox::velox) ) 



#### Create a function to strack the points 
extract_velox <- function(velox_Object, points){
  
  coords <- coordinates(points) %>%
    tbl_df() %>%
    rename(lat = V1, long = V2)
  
  velox_Object$extract(points, fun = function(x){ 
    x[x<0] <- NA
    x <- as.numeric(x)
    mean(x, na.rm = T)}) %>%
    tbl_df() %>%
    rename(values = V1) %>%
    bind_cols(coords) %>%
    mutate(i = 1:nrow(.)) %>%
    dplyr::select(i, lat, long, values)
}






GROC <- list.files(paste0(out_path, crop, '/GROC'), pattern = '.tif', 
                   full.names = TRUE)  %>%
  as.tibble %>%
  mutate(load_raster = purrr::map(.x = value, .f = function(.x){ 
    raster(.x) %>%  crop(extent(-180, 180, -50, 50))})) %>%
  mutate(raster_df = purrr::map(.x = load_raster, .f = ~future(velox(.x)))) %>%
  mutate(raster_df = purrr::map(raster_df, .f = ~value(.x))) %>% 
  mutate(points = purrr::map(.x = raster_df, .f = ~future(extract_velox(velox_Object = .x, points = sp_pdate)))) %>%
  mutate(points = purrr::map(points, ~value(.x))) 




##############



change_N <- function(.x, .y){
  rename_d <- .x %>%
    setNames(c("i", "long", "lat" , .y))
  return(rename_d)}



GROC_F <- GROC %>% 
  select(value, points) %>%
  mutate(name = list.files(paste0(out_path, crop, '/GROC'), pattern = '.tif') %>%
           #strsplit(., 'Mcalendar_')
           stringr::str_split('Mcalendar_', simplify = TRUE)%>%
           .[,2] %>%
           stringr::str_split('.tif', simplify = TRUE) %>%
           .[,1]) %>%
  select(name, points) %>%
  mutate(data = purrr::map2(.x = points, .y = name, .f = change_N))  %>%  
  select(data)  %>% 
  split(., f = 1:6) %>%
  lapply(., unnest) %>% 
  purrr::reduce(inner_join) 



Total_var <- inner_join(x = tbl, y = GROC_F %>% select(-lat, -long), by = 'i') 
write.csv(x = Total_var, file = paste0(out_path, crop, '/Total_vars.csv'), row.names = FALSE)

#### New raster 
dir.create(paste0(out_path, crop, '/GROC/summary'))

GROC_all <- Total_var %>% 
  names %>% 
  .[23:28] %>%
  as.list() %>%
  lapply(., rasterize_masa,  data = Total_var, raster = pdate) %>% 
  stack %>% 
  set_names(tbl %>%  
              names  %>% 
              .[23:28])




GROC_all %>% mean

writeRaster(x = GROC_all %>% mean, filename =  paste0(out_path, crop, '/GROC/summary/GROC_mean.tif'))

GROC_all %>% mean %>% rasterVis::gplot(.) + 
  geom_tile(aes(fill = value)) + 
  u + 
  coord_equal() +
  labs(x="Longitude",y="Latitude", fill = " ")   +
  scale_fill_distiller(palette = "RdYlGn", na.value="white", direction = 1) + 
  scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
  scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
  theme_bw() + theme(panel.background=element_rect(fill="white",colour="black")) 

ggsave( paste0(out_path ,crop,'/GROC/summary/GROC_mean.png'), width = 8, height = 3.5)




GROC_all %>% min
writeRaster(x = GROC_all %>% min, filename =  paste0(out_path, crop, '/GROC/summary/GROC_mim.tif'))

GROC_all %>% min %>% rasterVis::gplot(.) + 
  geom_tile(aes(fill = value)) + 
  u + 
  coord_equal() +
  labs(x="Longitude",y="Latitude", fill = " ")   +
  scale_fill_distiller(palette = "RdYlGn", na.value="white", direction = 1) + 
  scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
  scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
  theme_bw() + theme(panel.background=element_rect(fill="white",colour="black")) 

ggsave( paste0(out_path ,crop,'/GROC/summary/GROC_min.png'), width = 8, height = 3.5)







#### variograms and correlations.   


Total_var %>% 
  names %>% 
  .[23:28] %>% 
  as.list() %>% 
  lapply(all_variograms, tbl = Total_var)




##########          Correlations      ########## 


ngb <- 3
RI <- corLocal(GROC_all %>% min, all_inf$gap, ngb= ngb,   method = "pearson" )  

RI %>% 
  rasterVis::gplot(.) + 
  geom_tile(aes(fill = value)) + 
  u + 
  coord_equal() +
  labs(x="Longitude",y="Latitude", fill = " ")   +
  scale_fill_distiller(palette = "RdBu", na.value="white") + 
  scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
  scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
  theme_bw() + theme(panel.background=element_rect(fill="white",colour="black")) 


ggsave( paste0(out_path, crop,'/GROC/summary/Cor_min_gap_ngb_',ngb, '.png'), width = 8, height = 3.5)
writeRaster(x = RI, filename = paste0(out_path,crop,'/GROC/summary/Cor_min_gap_ngb_',ngb,'.tif'))


(RI > 0.5) %>% 
  rasterVis::gplot(.) + 
  geom_tile(aes(fill = value)) + 
  u + 
  coord_equal() +
  labs(x="Longitude",y="Latitude", fill = " ")   +
  scale_fill_distiller(palette =  "Blues", na.value="white", direction = 1) + 
  scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
  scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
  theme_bw() + theme(panel.background=element_rect(fill="white",colour="black"), legend.position = 'none') 

ggsave( paste0(out_path ,crop,'/GROC/summary/Cor_min_gap5_ngb_',ngb, '.png'), width = 8, height = 3.5)























##############################################################################
########### Tengo el harvest y los rendimientos potenciales... 
########### deberia evaluar si estan correlacionados o no? --- se van a realizar pruebas la proxima semana
########### 












