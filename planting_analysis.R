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


info <- 'data/'
pdate <- raster(paste0(info, 'Maize.crop.calendar.nc'), varname = 'plant') 
pdate_points <- rasterToPoints(pdate) %>%
  tbl_df()




## asignar la banda a que Año y mes pertenecen
n_bands <- nc_open(paste0(info, 'chirps-v2.0.monthly.nc'))$dim$time$len

dates_raster <- str_extract(nc_open(paste0(info, 'chirps-v2.0.monthly.nc'))$dim$time$units, "\\d{4}-\\d{1}-\\d{1}") %>%
  as.Date %>%
  seq(by = "month", length.out = n_bands) %>%
  data_frame(date = .) %>%
  mutate(year = year(date), month = month(date)) %>%
  mutate(band = 1:n_bands, file = rep(paste0(info, 'chirps-v2.0.monthly.nc'), n_bands)) %>%
  filter(row_number()<=4) %>%
  mutate(load_raster = map2(.x = file, .y = band, .f = raster)) %>%
  mutate(raster_df = map(.x = load_raster, .f = velox::velox))



# dates <- seq(dates_raster, by = "month", length.out = n_bands)


# nc_open(paste0(info, 'chirps-v2.0.monthly.nc'))$dim$time$units

months <- 1:12
ncmeta::nc_dim(paste0(info, 'chirps-v2.0.monthly.nc'), 2)
filename = paste0(info, 'chirps-v2.0.monthly.nc')
nbands()
n_bands <- nc_open(paste0(info, 'chirps-v2.0.monthly.nc'))$dim$time$len

ncvar_get(nc)
tidync(x  = paste0(info, 'chirps-v2.0.monthly.nc')) %>% 
  hyper_tibble()
num_bands <- 1:2

x <- raster(paste0(info, 'chirps-v2.0.monthly.nc'))

y <- map2(.x = paste0(info, 'chirps-v2.0.monthly.nc'), .y = num_bands, .f = raster)

x <- map(.x = y, .f = velox::velox)
x <- velox(x)
x$extract(sp_pdate, fun = mean)

sp_pdate <- rasterToPolygons(pdate,  dissolve = F) 
x$extract(sp_pdate, fun = mean)



map(.x = y, .f = velox::velox)

map2(.x = x$extract, .f = mean)


values <- x$extract(y, fun = mean)

lapply(paste0(info, 'chirps-v2.0.monthly.nc'), raster, band = num_bands)
date <- magrittr::extract2(x@z, 1)
raster_month <- month(date)

raster::extract(x)



velox(paste0(info, 'chirps-v2.0.monthly.nc'), band = 1)
