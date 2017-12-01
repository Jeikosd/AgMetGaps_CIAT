##################################################################
##########                    Packages                  ########## 
##################################################################


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


# tibbletime


setwd('/mnt/workspace_cluster_9/AgMetGaps')


##################################################################
#######                   Planting dates                  ######## 
##################################################################


## Proof for maize information 

# D:/AgMaps/planting_dates/calendar/


pdate <- raster(paste0('Inputs/05-Crop Calendar Sacks/', 'Rice.crop.calendar.nc'), varname = 'plant') %>% 
  crop(extent(-180, 180, -50, 50)) 


pdate_points <- rasterToPoints(pdate) %>%
  tbl_df() %>% 
  setNames(nm = c('x', 'y', 'julian.start')) %>%
  mutate(Planting =  month.day.year(julian.start)$month)


hdate <- raster(paste0('Inputs/05-Crop Calendar Sacks/', 'Rice.crop.calendar.nc'), varname = 'harvest') %>% 
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
  
  if(flor>12){flor <- flor - 12} else if( flor <= 12){flor <- flor}
  
  return(flor)}




month_below <- function(.x){
  if(.x > 1) { below <- .x - 1} else if(.x  == 1){ below <- 12 }
  return(below)}



month_above <- function(.x){
  if(.x < 12) { above <- .x + 1} else if(.x  == 12){ above <- 1 }
  return(above)}





crop.time <- inner_join(pdate_points, hdate_points) %>% 
  select(-julian.start, -julian.h) %>% 
  mutate(flor = map2(.x = Planting, .y = harvest, .f = compute_flor)) %>% 
  unnest() %>% 
  select(x, y, Planting, flor, harvest) %>% 
  mutate(a.Planting1 = map(.x = Planting, .f = month_below), 
         a.Planting3 = map(.x = Planting, .f = month_above), 
         b.flor1 = map(.x = flor, .f = month_below), 
         b.flor3 = map(.x = flor, .f = month_above),
         c.harvest1= map(.x = harvest, .f = month_below), 
         c.harvest3 = map(.x = harvest, .f = month_above)) %>% 
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
##########      convert to raster if is necessary.      ########## 
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

##############################
##############################






##################################################################
##################################################################
##########        Climate:  variability  analysis       ########## 
##################################################################
##################################################################



##################################################################
##########        Precipitation: Chirps                 ########## 
##################################################################

# 'D:/AgMaps/Chips_Monthly/'
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
    mutate(load_raster = map2(.x = file, .y = band, .f = ~future(raster_mod(.x,.y)))) %>%
    mutate(load_raster = map(.x = load_raster, .f = ~value(.x))) %>% 
    mutate(raster_df = map(.x = load_raster, .f = ~future(velox(.x)))) %>%
    mutate(raster_df = map(raster_df, .f = ~value(.x))) 
)
  
  


#### Extract points
# plan(multisession, workers = availableCores() - 4)

system.time(  
  dates_raster <- dates_raster %>% 
  mutate(points = map(.x = raster_df, .f = ~future(extract_velox(velox_Object = .x, points = sp_pdate)))) %>%
  mutate(points = map(points, ~value(.x))) 
)






##### Understand the extract information for each pixel 

# This part extact only the information for year, 
# month and important points


##### Understand the jeison's scrip



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






### This function extract for each pixel the chirps information 
### i represents the exactly pixel 

make_station <- function(x, out, name){
  
  file_name <- paste0( out, name )
  
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
    select(-lat.y, -long.y) %>%
    group_by(phase, year, lat.x, long.x) %>%
    rename(lat = lat.x, long = long.x, precip =  values) %>% 
    summarise(prec_clim = sum(precip)) %>%
    ungroup()
  
  return(proof)}

extract_months <- function( dates, atelier, out1, name){
  
  folder <- atelier %>% 
    select(phase) %>% 
    filter(row_number() == 1) %>% 
    as.character
  
  
  out <- paste0(out1, folder, '/')
  if(dir.exists(out) ==  'FALSE'){dir.create(paste0(out1, folder, '/'))}else if(dir.exists(out) ==  'TRUE'){print('TRUE')}
  
  
  
  ### Is necessary parallelize the process?
  test <-point_dates %>% 
    mutate(i = 1:nrow(.)) %>%
    nest(-i) %>% 
    .[1:100, ]%>%
    mutate(stations = map(.x =  data , .f =  make_station, out = out, name =  name)) %>% 
    mutate(each_Pclim =  purrr::map(.x =  stations, .f = point_extract, y = atelier))
  
  return(test) }




calendar <- crop.time 


out1 <- '/mnt/workspace_cluster_9/AgMetGaps/monthly_out/rice/precip/'
name <- 'rice_precip'


planting <- crop.time %>% 
  filter(phase == 'a') %>% 
  rename(lat = x,  long =  y) %>%
  group_by(type) %>%
  mutate(id = 1:length(type)) %>%
  ungroup()





atelier <- planting



test <-  extract_months(dates = point_dates, atelier = planting, out = out1, name = name)





# creation to the climatology
test %>% 
  select( i, each_Pclim) %>% 
  unnest %>% 
  group_by(i, lat,  long, phase) %>%
  summarise(climatology_prec = mean(prec_clim), sd.prec = sd(prec_clim)) %>%
  mutate(cv.prec = (sd.prec/climatology_prec) * 100) %>% 
  ungroup

















##################################################################
##########     Mean Temperature: NCEP CPC GHCN_CAMS     ########## 
##################################################################




# for this variable is necesary create a diferent 

# temporaly 
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

  
  

# 'D:/AgMaps/GHCN_CAMS/'
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
  mutate(load_raster = map2(.x = file, .y = band, .f = ~future(raster_mod_temp(.x,.y)))) %>%
  mutate(load_raster = map(.x = load_raster, .f = ~value(.x))) %>% 
  mutate(raster_df = map(.x = load_raster, .f = ~future(velox(.x)))) %>%
  mutate(raster_df = map(raster_df, .f = ~value(.x))) %>% 
  mutate(points = map(.x = raster_df, .f = ~future(extract_velox_temp(velox_Object = .x, points = sp_pdate)))) %>%
  mutate(points = map(points, ~value(.x))) 
)










##################################################################
##################################################################
##########                   Yield Gaps                 ########## 
##################################################################
##################################################################


Yield.G <- list.files(path = 'Inputs/Yield_Gaps_ClimateBins/rice_yieldgap_netcdf/', 
                      pattern = 'YieldGap.nc', full.names = TRUE) %>%
  as.tibble %>% 
  mutate(load_raster = map(.x = value, .f = function(.x){ 
    raster(.x) %>%  crop(extent(-180, 180, -50, 50))})) %>%
  mutate(raster_df =  map(.x = load_raster, .f = velox::velox) ) 

# mutate(points = map(.x = raster_df, .f = ~future(extract_velox_temp(velox_Object = .x, points = sp_pdate))))

coords <- coordinates(sp_pdate) %>%
  tbl_df() %>%
  rename(lat = V1, long = V2)

test <- Yield.G$raster_df[[1]]$extract(sp_pdate, fun = function(x){ 
  mean(x, na.rm = T)}) %>%
  tbl_df() %>%
  rename(gap = V1) %>%
  bind_cols(coords) %>%
  dplyr::select(lat, long, gap)


test[which(test$gap[] == 'NaN'), 3] <- NA











##################################################################
##################################################################
##########        proof : first idea to analysis        ########## 
##################################################################
##################################################################

# temporal 


grap_variability <-  inner_join(cropV.temp , cropV.prec)  %>% 
  mutate(value1 =  value)  %>%
  separate(value1, c('month', 'other'), '_')  %>%
  select(-month) %>%
  nest(-other) %>% 
  select(data) %>% 
  unlist(recursive = F)  %>% 
  map(.x = . , .f = function(.x){
    name <- .x$value[1]     
    t <- .x %>% 
      set_names(., paste0(name, '.', names(.x))) %>%
      select(-1) 
    return(t)}) %>% 
  bind_cols() %>% 
  rename(., lat =  month_flor.lat , long = month_flor.long) %>% 
  select(-month_h.long, -month_h.lat,-month_start.lat ,-month_start.long) %>%
  left_join(test, .)  







# knitr



# correlation 

grap_variability %>% 
  select(-lat, -long) %>% 
  na.omit   %>%  # temporal
  cor %>% 
  abs %>% 
  write.csv(., file = 'test.csv')