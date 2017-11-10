# In linux cd /mnt/workspace_cluster_9/AgMetGaps/

# /mnt/workspace_cluster_9/AgMetGaps/

setwd('C:/Users/AESQUIVEL/AppData/Roaming/Microsoft/Windows/Network Shortcuts/AgMetGaps')

##################################################################
##########                    Packages                  ########## 
##################################################################


library(raster)
library(ncdf4)
library(dplyr)
library(ggplot2)
library(velox)
library(foreach)
library(future)
library(doFuture)
library(purrr)
library(magrittr)
library(lubridate)
# library(tidync)
library(tidyr)
library(readr)
library(stringr)
library(tidyverse)
library(chron)

library(future)  # parallel package

# tibbletime


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
  mutate(month_start =  month.day.year(julian.start)$month)


hdate <- raster(paste0('Inputs/05-Crop Calendar Sacks/', 'Rice.crop.calendar.nc'), varname = 'harvest') %>% 
  crop(extent(-180, 180, -50, 50)) 

hdate_points <- rasterToPoints(hdate) %>%
  tbl_df() %>% 
  setNames(nm = c('x', 'y', 'julian.h')) %>%
  mutate(month_h =  month.day.year(julian.h)$month) 


date_points <- inner_join(pdate_points, hdate_points)


crop.time <- date_points %>% 
 select(month_start, month_h)  %>% 
  count(month_start, month_h) %>% 
  filter(n == max(n)) %>% 
  # arreglar en el caso de segundo semestre
  mutate(month.flor = if( month_h > month_start){
    round( month_start + (month_h - month_start)/2, 0)
  }else if( month_h < month_start){
    round(month_start + ((12-month_start) +  month_h) / 2 , 0)
  }) %>% 
  mutate(month.flor = if(month.flor>12){month.flor -12} else if(month.flor<=12){month.flor}) %>%
  select(month_start, month.flor, month_h) %>%
  gather(season, month.2)  %>% # lo mismo que en el paso anterior 
  mutate(month.1 = ifelse(month.2 == 1, 12, month.2 - 1) , month.3 = ifelse(month.2 == 12, 1, month.2 + 1)) %>%
  gather(possition, month, -season) %>%
  arrange(., group_by = season)


# write.csv(x = crop.time, file = 'monthly_out/Rice_first.csv', row.names = NULL)





##############################
##############################
# To use the spatial polygons information in the velox strack to points 
sp_pdate <- rasterToPolygons(pdate,  dissolve = F) 







##################################################################
##########        Precipitation: Chirps                 ########## 
##################################################################

# 'D:/AgMaps/Chips_Monthly/'
## asignar la banda a que Año y mes pertenecen
n_bands <- nc_open(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'))$dim$time$len
n_bands <- n_bands - 8 # temporal



#### Create a function to strack the points 
extract_velox <- function(velox_Object, points){
 
  coords <- coordinates(points) %>%
    tbl_df() %>%
    rename(lat = V1, long = V2)
  
  velox_Object$extract(points, fun = mean) %>%
    tbl_df() %>%
    rename(values = V1) %>%
    bind_cols(coords) %>%
    dplyr::select(lat, long, values)
  
}



raster_mod <- function(.x, .y){
  raster(.x, band = .y)
}



## read monthly precipitation rasters with furure functions 
plan(multisession, workers = availableCores() - 4)
  
  
system.time(  
  ## read monthly precipitation rasters 
  dates_raster <- str_extract(nc_open(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'))$dim$time$units, "\\d{4}-\\d{1}-\\d{1}") %>%
    as.Date() %>%
    seq(by = "month", length.out = n_bands) %>%
    data_frame(date = .) %>%
    mutate(year = year(date), month = month(date)) %>%
    mutate(band = 1:n_bands, file = rep(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'), n_bands)) %>%
    filter(month %in% crop.time$month)   %>%  #  filter(row_number() < 2) %>% 
    mutate(load_raster = map2(.x = file, .y = band, .f = ~future(raster_mod(.x,.y)))) %>%
    mutate(load_raster = map(.x = load_raster, .f = ~value(.x))) %>% 
    mutate(raster_df = map(.x = load_raster, .f = ~future(velox(.x)))) %>%
    mutate(raster_df = map(raster_df, .f = ~value(.x))) 
)
  
  




#### proof1 extract points 
plan(multisession, workers = availableCores() - 4)

system.time(  
  dates_raster <- dates_raster %>% 
  mutate(points = map(.x = raster_df, .f = ~future(extract_velox(velox_Object = .x, points = sp_pdate)))) %>%
  mutate(points = map(points, ~value(.x))) 
)






#### The idea of this function is make de summary of the one season


time.levels <- crop.time %>% 
  .$season %>%
  factor %>% 
  levels %>% 
  as.tibble()


# climatology function to calculate the climate variability in the crop season (mean, sd and cv)

climatology <-  function(levels){

  data  <-  dates_raster %>%
    mutate(., time = ifelse( 
      month %in% crop.time$month[crop.time$season == as.character(levels)], 'TRUE' , 'FALSE')) %>%
    filter(time == TRUE)   %>% 
    select(year, month, points) %>% 
    unnest  %>% 
    group_by(year, lat, long )  %>% 
    summarise(add =  sum(values))  %>%
    ungroup %>% 
    group_by(lat, long)  %>%
    summarise(climatology =  mean(add), sd.p =  var(add))  %>%
    mutate(cv.p = sd.p /climatology * 100) 

    # nest(-year)
return(data)}


cropV.prec <-  time.levels %>% 
  mutate(data =  map(.x = value, .f = climatology)) %>% 
  unnest








##################################################################
##########     Mean Temperature: NCEP CPC GHCN_CAMS     ########## 
##################################################################


# 'D:/AgMaps/GHCN_CAMS/'
## asignar la banda a que Año y mes pertenecen
n_bands <- nc_open(paste0('D:/AgMaps/GHCN_CAMS/', 'data.nc'))$dim$T$len
n_bands <- 36*12 # temporal
  

# this data is available from this date, but we download to 1980 the same start-date to chirps 
# nc_open(paste0('Inputs/GHCN_CAMS/', 'data.nc'))$dim$T$units 



GHCN_CAMS <- str_extract(nc_open(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'))$dim$time$units, "\\d{4}-\\d{1}-\\d{1}") %>%
  as.Date() %>%
  seq(by = "month", length.out = n_bands)  %>%
  data_frame(date = .) %>%
  mutate(year = year(date), month = month(date)) %>%
  mutate(band = 1:n_bands, file = rep(paste0('Inputs/GHCN_CAMS/', 'data.nc'), n_bands)) # %>%
  