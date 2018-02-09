## =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= ##
## Hot-spots analysis
## =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= ##

rm(list = ls()) ; gc(reset = TRUE)


## =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= ##
## Packages
## =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= ##

suppressMessages(if(!require(tidyverse)){install.packages('tidyverse'); library(tidyverse)} else {library(tidyverse)})
suppressMessages(if(!require(raster)){install.packages('raster'); library(raster)} else {library(raster)})
suppressMessages(if(!require(ncdf4)){install.packages('ncdf4'); library(ncdf4)} else {library(ncdf4)})
suppressMessages(if(!require(velox)){install.packages('velox'); library(velox)} else {library(velox)})
suppressMessages(if(!require(future)){install.packages('future'); library(future)} else {library(future)}) # parallel package
suppressMessages(if(!require(doFuture)){install.packages('doFuture'); library(doFuture)} else {library(doFuture)})
suppressMessages(if(!require(sf)){install.packages('sf'); library(sf)} else {library(sf)})



###### Graphical parameters -----  to ggplot2

### Lectura del shp

shp <- read_sf("/mnt/workspace_cluster_9/AgMetGaps/Inputs/shp/mapa_mundi.shp")  %>%
  as('Spatial') %>% 
  crop(extent(-180, 180, -50, 50))

# u <- borders(shp, colour="black")

ewbrks <- c(seq(-180,0,45), seq(0, 180, 45))
nsbrks <- seq(-50,50,25)
ewlbls <- unlist(lapply(ewbrks, function(x) ifelse(x < 0, paste(abs(x), "°W"), ifelse(x > 0, paste( abs(x), "°E"),x))))
nslbls <- unlist(lapply(nsbrks, function(x) ifelse(x < 0, paste(abs(x), "°S"), ifelse(x > 0, paste(abs(x), "°N"),x))))

Blues<-colorRampPalette(c('#fff7fb','#ece7f2','#d0d1e6','#a6bddb','#74a9cf','#3690c0','#0570b0','#045a8d','#023858','#233159'))





## =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= ##
#######                   Preliminar parameters                 ######## 
## =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= ##

path <- "/mnt/workspace_cluster_9/AgMetGaps/Inputs/05-Crop Calendar Sacks/"
crop <- 'wheat' # rice , maize , wheat
crop_type <- 'Wheat.crop.calendar.nc' # rice , maize , wheat
out_path <- '/mnt/workspace_cluster_9/AgMetGaps/monthly_out/'




## =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= ##
#######                   Planting dates                  ######## 
## =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= ##


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




### Example graph --  ggplot with stack data

p <- stack(pdate, hdate) 
p <- as.data.frame(p, xy=TRUE) %>%  gather( correlation, value, -x, -y)


ggplot() + 
  geom_tile(data = p ,aes(x = x, y = y, fill = value)) + 
  geom_polygon(data = shp, aes(x=long, y = lat, group = group), color = "gray", fill=NA)  +
  coord_equal() +
  labs(x="Longitude",y="Latitude", fill = " ")   +
  facet_wrap(~correlation) + 
  scale_fill_distiller(palette = "Spectral", na.value="white", direction = 1)  +
  scale_x_continuous(breaks = ewbrks, labels = ewlbls, expand = c(0, 0)) +
  scale_y_continuous(breaks = nsbrks, labels = nslbls, expand = c(0, 0)) +
  theme_bw() + theme(panel.background=element_rect(fill="white",colour="black"))


