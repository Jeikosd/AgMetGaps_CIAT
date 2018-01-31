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
    
    
    test1 <- as.Date('1981300',  format("%Y%j"))
    test2 <- as.Date('1982032',  format("%Y%j"))
    as.numeric(difftime(as.POSIXct(test2), units = 'days')       )
    as.numeric(test2-test1)
    
    
    summary_out <- suppressWarnings(read_summary(dir_run_id)) %>%
      mutate(yield_0 = HWAH,
             d_dry = as.numeric(as.Date(MDAT, format("%Y%j"))-as.Date(PDAT, format("%Y%j"))),
             prec_acu = PRCP,
             bio_acu = CWAM)     
    
    
    library ( ff )
    N <- 1000 # sample size #
    n <- 100 # chunk size #
    years <- 2000 : 2009
    types <- factor ( c ( " A " , " B " ," C " ) )    
    
    Year <- ff ( years , vmode = 'ushort', length = N, update = FALSE ,
                 filename = "D:/Year.ff" , finalizer = "close" )
    
    for ( i in chunk ( 1, N, n ) ){
      Year[i] <- sample(years, sum (i) , TRUE)
      
    }
    
    
    mat <- ff(vmode ="double", dim = c(ncell(STACK), nlayers(STACK)), filename=ff_path)
    
    for(i in 1:nlayers(STACK)){
      mat[,i] <- STACK[[i]][]
    }
    save(mat,file=paste0(getwd(),"/data.RData"))
    
    
    lapply(1:length(df_values_all), function(df) df_values_all[sapply(df_values_all, is.data.frame)])[[1]]
    
    
    x <- rnorm(100)
    
    y <- x + rnorm(100)
    
    z <-  y - x    
    
    cor(z, y)
    cor(x, y)
    
    
    
    
    b <- stack(system.file("external/rlogo.grd", package="raster"))
    b <- aggregate(b, 20, mean)
    
    set.seed(0)
    b[[2]] <- flip(b[[2]], 'y') + runif(ncell(b))
    b[[1]] <- b[[1]] + runif(ncell(b))
    
    x <- corLocal(b[[1]], b[[2]], test=FALSE, ngb=3)
    getValues(x)
    y1 <- focal(b[[1]], w=matrix(1/9, nc=3, nr=3))
    y2 <- focal(b[[2]], w=matrix(1/9, nc=3, nr=3))
    y3 <- focal(b[[3]], w=matrix(1/9, nc=3, nr=3))
    
    
    cor(c(210.1871, 144.8546, 189.6567, 136.5504))
    
    getValues(y)
    getValues(y1)
    getValues(y2)
    
    cor(c(180.6499,	167.9311,
          181.6695,	167.3404
    ), c(184.9532,	173.1772,
         183.3251,	172.6845))
    
    plot(b[[1]], zlim = c(100,200))
    plot(y)
    
    
    
    
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
    
    # chirps_path <- '//dapadfs/data_cluster_4/observed/gridded_products/chirps/daily/'
    # chirps_file <- list.files(chirps_path, pattern = '.tif$', full.names = T)
    # points_path <- '//dapadfs/workspace_cluster_9/AgMetGaps/weather_analysis/spatial_points/Maize'
    # points_file <- list.files(points_path, pattern = '.geojson$', full.names = T)
    # out_file <- '//dapadfs/workspace_cluster_9/AgMetGaps/weather_analysis/precipitation_points/daily_chirps_csv/'
    
    
    
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
    
    
    # local_cpu <- rep("localhost", availableCores() - 1)
    # external_cpu <- rep("caribe.ciat.cgiar.org", 8)  # server donde trabaja Alejandra
    # external_cpu <- rep("climate.ciat.cgiar.org", each = 10)
    
    # workers <- c(local_cpu, external_cpu)
    # options(future.globals.maxSize= 891289600)
    options(future.globals.maxSize = 31912896000)
    plan(multisession, workers = 10)
    # plan(cluster, workers = workers)
    
    # plan(list(tweak(cluster, workers = workers), multicore))
    
    # extract_chirps <- listenv::listenv()
    # tic("extract the chirps information")
    # extract_chirps <- future::future_lapply(x, FUN = extract_velox, points = geo_files, out_file) 
    # toc()
    
    
    # strategy <- "future::multisession"
    
    
    
    
    mean_point <- function(x){
      
      x[x<0] <- NA
      mean(x, na.rm = T)
      
    }
    distribute_load(x = 25000, n = 10)
    
    
    distribute_load <- function(x, n = get_number_of_threads()) {
      assertthat::assert_that(assertthat::is.count(x),
                              assertthat::is.count(n),
                              isTRUE(x > 0),
                              isTRUE(n > 0))
      if (n == 1) {
        i <- list(seq_len(x))
      } else if (x <= n) {
        i <- as.list(seq_len(x))
      } else {
        j <- as.integer(floor(seq(1, n+1 , length.out = x + 1)))
        i <- list()
        for (k in seq_len(n)) {
          i[[k]] <- which(j == k)
        }
      }
      
      i
    }
    
    
    stack_future <- function(x) {
      
      x <- future.apply::future_lapply(X = x, FUN = raster) %>%
        raster::stack(x) %>%
        velox::velox()
      
      return(x)
      
      
    }
    da
    
    extract_velox <- function(file, points, out_file){
      
      file <- x[1:500]
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
      # velox(file[1], extent=c(0,1,0,1))
      plan(multisession, workers = 10)
      plan(list(tweak(multisession, workers = 2), tweak(multisession, workers = 4)))
      tic('parallel map raster')
      vx_raster <- purrr::map(.x = file, .f = ~future(raster(.x))) %>%
        future::values() %>%
        raster::stack()
      vx_raster <- velox(vx_raster)
      toc()  ## tomo 4.5 mins
      
      ## haciendo load balancing
      
      l = distribute_load(x = length(file), n = 10)
      
      files <- purrr::map(.x = l, .f = function(l, x) x[l], file)
  
      tic('parallel balancing velox')
      # vx_raster <- purrr::map(.x = files, .f = ~future(velox(raster::stack(.x)))) %>%
      vx_raster <- future.apply::future_lapply(X = files, FUN = function(x) velox(raster::stack(x))) %>%
        # future::values() %>%
        velox::velox()
      toc() 
      
      stack_future 
      
      tic('parallel balancing velox')
      # vx_raster <- purrr::map(.x = files, .f = ~future(velox(raster::stack(.x)))) %>%
      vx_raster <- future.apply::future_lapply(X = files, FUN = stack_future) %>%
        # future::values() %>%
        velox::velox()
      toc() 
      
      m <- plyr::llply(distribute_load(length(y)), .parallel = TRUE,
                       function(i) {
                         return(do.call(velox_extract, append(
                           list(x = x, y = y[i, ], fun = fun), args)))
                       })
      
      
      # vx_raster <- addLayer(vx_raster , y)  # puede ser una buena propuesta
      
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
      fast_extract <- function( x, points, fun){
        x$extract(points, fun = fun) %>%
          tbl_df()
      }
      
      
      fast_extract(vx_raster, points[1, ], fun = mean_point)
      tic("normal extract")
      vx_raster$extract(points, fun = mean_point)
      toc()
      
      
      ### la mejor opcion es hacerlo con load balancing 
      tic("parallel extract")
      
      z <- future.apply::future_lapply(X = l , FUN = function(i) {fast_extract(vx_raster, points[i, ], mean_point)})
      toc()
      
      
      lapply(X = 1:2, FUN = function(i){fast_extract(vx_raster, points[i, ], mean_point)})
      
      z <- listenv()
      for (ii in 1:3) {
             z[[ii]] %<-% {
               fast_extract(vx_raster, points[i, ], mean_point)
               }
         }
      
      
      z <- purrr::map(.x = points, .f = ~future(fast_extract(y, .x, fun = mean_point))) %>%
        future::values() 
      format(object.size(y), units = "GiB")
      
      
      
      distribute_load <- function(x, n=get_number_of_threads()) {
        assertthat::assert_that(assertthat::is.count(x),
                                assertthat::is.count(n),
                                isTRUE(x > 0),
                                isTRUE(n > 0))
        if (n == 1) {
          i <- list(seq_len(x))
        } else if (x <= n) {
          i <- as.list(seq_len(x))
        } else {
          j <- as.integer(floor(seq(1, n + 1, length.out = x + 1)))
          i <- list()
          for (k in seq_len(n)) {
            i[[k]] <- which(j == k)
          }
        }
        i
      }
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
    