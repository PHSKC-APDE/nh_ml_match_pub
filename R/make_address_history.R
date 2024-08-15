make_address_history = function(){
  con = hhsaw()
  ah = dbGetQuery(con, 
                  "select distinct l.source_id, l.source_system, 
                  l.zip, l.geo_hash_geocode, g.geo_add_type, g.geo_add_type_grade,
                  g.geo_lon_2926 as lon, g.geo_lat_2926 as lat
                  from noharms.addresses as l
                  left join ref.address_geocode as g on l.geo_hash_geocode = g.geo_hash_geocode
                  where zip is NOT NULL 
                  OR 
                  g.geo_add_type_grade in ('A','B','C','D','E','F')
                  ")
  setDT(ah)
  
  # Prep zip codes
  zh = unique(ah[!is.na(zip), .(source_id, source_system, zip)])
  zzz = stringr::str_extract_all(zh$zip, "[0-9]+")
  zzz = lapply(zzz, paste, collapse = "")
  zzz = substr(zzz, 1, 5)
  zh[, `:=`(zip, zzz)]
  
  ah = unique(ah[geo_add_type_grade %in% c('A','B','C','D','E','F') & !is.na(lon)])
  ahxy = sf::st_as_sf(ah, coords = c('lon', 'lat'), crs = 2926)
  
  list(zh = zh, ah = ahxy)
}