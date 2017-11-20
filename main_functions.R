
## main functions

## funcion para crear puntos como poligonos (formato que necesita el paquete velox para extraer puntos de raster)

# file_nc: sacks nc planting
# out_path: folder to save the information
# extent: extent if it is necessary to crop 
# raster_source: source of the raster 
# ...: to pass raster function

raster_Polygons <- function(file_nc, out_path, extent, raster_source, ...){
  
  # file_nc <- paste0(path, planting)
  # out_path 
  # extent <- extent_information 
  
  crop_name <- stringr::str_extract(basename(file_nc), pattern = '[^.]+')
  
  calendar_points <- suppressWarnings(raster::raster(file_nc, ...))
  # start_planting <- raster::raster(file_nc, varname = 'plant.start')
  # end_planting <- raster::raster(file_nc, varname = 'plant.end')
  
  ### cropping with the climate information (chirps or agmerra) 
  ### and make polygons for velox package
  
  points_df <- make_polygons(calendar_points, extent_information)
  # setwd(out_path)
  points_df <- sf::st_as_sf(points_df)
  
  # as(points_df, "Spatial")
  
  out_points <- mkdirs(fp = paste0(out_path, crop_name))
  
  st_write(points_df, dsn = out_points,
           # layer = "rice_points_sacks.shp",
           layer = paste0(crop_name, '_', raster_source, '.shp'),
           driver = "ESRI Shapefile",
           delete_layer = TRUE)
           # delete_dsn = TRUE)
}

  

## x:  a raster file
## extent: extent to crop the raster file 

make_polygons <- function(x, extent){
  
  temp_raster <- rasterTmpFile()
  
  exchange <- names(x)
  variable <- stringr::str_replace_all(names(x), pattern = '[:punct:]+', replacement  = '_')
  
  points <- x %>%
    raster::crop(extent) %>%
    rasterToPoints() %>%
    dplyr::tbl_df() %>%
    dplyr::select(x, y) %>%
    dplyr::rename(lat = x, long = y)
  
  polygons <- x %>%
    raster::crop(extent) %>%
    raster::writeRaster(filename = temp_raster, overwrite = TRUE) %>%
    raster::rasterToPolygons(dissolve = F) 
  
  polygons@data = data.frame(polygons@data, data.frame(points)) %>%
    dplyr::tbl_df() %>%
    dplyr::rename(!!variable :=  !!exchange) %>%
    select(lat, long, !!variable)
  
  
  return(polygons)
  # x <- plant_start
  
  
}

# fp: folder to make

mkdirs <- function(fp) {
  
  if(!file.exists(fp)) {
    mkdirs(dirname(fp))
    dir.create(fp, showWarnings = F, mode = "777")
    
  }
  
  return(fp)
} 

  
  
  
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
  