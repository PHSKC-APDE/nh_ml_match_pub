add_zips_to_db = function(duck, zippath = "[FILEPATH REDACTED]Shapefiles_protected/ZIP/adci_wa_zip_confidential.shp", ...){
  
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1])
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  loadspatial(ddb)
  
  zips <- sf::read_sf(zippath)
  zips <- zips %>% group_by(POSTCOD) %>% summarize()
  zips <- sf::st_centroid(zips)
  zips = st_transform(zips, 2926)
  names(zips) <- c('ZIP', 'geometry')
  
  temp = tempfile(fileext = '.geojson')
  sf::st_write(zips, temp)
  
  dbExecute(ddb, glue::glue_sql("
  create or replace table zip_centriods as
  select * from st_read({temp})", .con = ddb))
  
  return(rlang::hash(zips))
  
}
