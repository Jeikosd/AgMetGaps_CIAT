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
  # ggplot(., aes(as.factor(month_h), y = n)) + geom_bar(stat = 'identity')
  filter(n == max(n)) %>% 
  # arreglar en el caso de segundo semestre
    mutate(month.flor = month_start + (month_h - month_start)/2) %>% 
  select(month_start, month.flor, month_h) %>%
  gather(season, middle)  %>% # lo mismo que en el paso anterior 
  mutate(month.1 = middle - 1 , month.2 =  middle + 1) %>%
  gather(possition, month, -season) %>%
  arrange(., group_by = season)


# write.csv(x = crop.time, file = 'Rice_first.csv')



##################################################################
##########        Precipitation: Chirps                 ########## 
##################################################################

# 'D:/AgMaps/Chips_Monthly/'
## asignar la banda a que Año y mes pertenecen
n_bands <- nc_open(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'))$dim$time$len
n_bands <- n_bands - 8 # temporal



# To use the spatial polygons information in the velox strack to points 
sp_pdate <- rasterToPolygons(pdate,  dissolve = F) 




#### Create a function to strack the points 
velox.points <- function(velox.data, sp.I){
  velox_points <- velox.data$extract(sp=sp_pdate, fun=mean)  
  # points <- bind_cols(sp_pdate %>% coordinates, velox_points)
return(points)}











## read monthly precipitation rasters 
dates_raster <- str_extract(nc_open(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'))$dim$time$units, "\\d{4}-\\d{1}-\\d{1}") %>%
  as.Date() %>%
  seq(by = "month", length.out = n_bands) %>%
  data_frame(date = .) %>%
  mutate(year = year(date), month = month(date)) %>%
  mutate(band = 1:n_bands, file = rep(paste0('Chips_Monthly/', 'chirps-v2.0.monthly.nc'), n_bands)) %>%
  filter(month %in% crop.time$month) %>% #  filter(row_number() < 5) %>% 
  mutate(load_raster = map2(.x = file, .y = band, .f = raster)) %>%
  mutate(raster_df = map(.x = load_raster, .f = velox::velox)) # Creating VeloxRaster Objects
  




start <- dates_raster %>% 
  mutate(., month_start = ifelse( month %in% crop.time$month[crop.time$season == 'month_start'],
                                  'TRUE' , 'FALSE')) %>%
  # group_by(month_start, year) %>% 
  filter(month_start == TRUE) %>% 
  group_by(year) %>% 
  select(-month_start)




#### proof1 extract points 
dates_raster %>% 
  mutate(points.Crop = map2(.x = raster_df, .y = sp_pdate, .f = velox.points)) # aqui estoy trabajando





