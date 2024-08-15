#' Generate name list columns by ssn
#' @param duck the output of init_duck, specifically, the file path to the duckdb
#' @param data_version data version
create_ssn_name_lists = function(duck, data_version, ...){
  ddb = dbConnect(duckdb(), dbdir = duck[1])
  on.exit({
    DBI::dbDisconnect(ddb, shutdown = TRUE)
  })
  dtab = DBI::Id(table = paste0('data_',data_version))
  ntab = DBI::Id(table = paste0('ssnnames_', data_version))
  # Make the list of possible first names and last names by ssn
  q = glue::glue_sql(
    "

    create or replace table {`ntab`} as 
    select f.ssn as ssn, ssn_first_name_list, ssn_last_name_list from
    (select ssn, list(first_name_noblank) as ssn_first_name_list from 
    (select distinct first_name_noblank, ssn from {`dtab`}) as a
    where len(ssn)>=7
    group by ssn) as f
    full join (select ssn, list(last_name_noblank) as ssn_last_name_list from 
    (select distinct last_name_noblank, ssn from {`dtab`}) as a
    where len(ssn)>=7
    group by ssn) as l on f.ssn = l.ssn
    

    ",.con = ddb)
  
  r = dbExecute(ddb,q)
  
  
  return(dbGetQuery(ddb, glue::glue_sql('select count(*) as N from {`ntab`}', .con = ddb))$N)
}


#' Generate name list columns by source system and source_id
#' @param duck the output of init_duck, specifically, the file path to the duckdb
#' @param data_version data version
create_ssid_name_lists = function(duck, data_version, ...){
  ddb = dbConnect(duckdb(), dbdir = duck[1])
  on.exit({
    DBI::dbDisconnect(ddb, shutdown = TRUE)
  })
  dtab = DBI::Id(table = paste0('data_',data_version))
  ntab = DBI::Id(table = paste0('ssidnames_', data_version))
  # Make the list of possible first names and last names by ssid
  q = glue::glue_sql(
    "

    create or replace table {`ntab`} as 
    select f.sssi_id, ssid_first_name_list, ssid_last_name_list from
    (select sssi_id, list(first_name_noblank) as ssid_first_name_list from 
    (select distinct first_name_noblank, sssi_id from {`dtab`}) as a
    group by sssi_id) as f
    full join (select sssi_id, list(last_name_noblank) as ssid_last_name_list from 
    (select distinct last_name_noblank, sssi_id from {`dtab`}) as a
    group by sssi_id) as l 
    USING(sssi_id)
    

    ",.con = ddb)
  
  r = dbExecute(ddb,q)
  
  
  return(dbGetQuery(ddb, glue::glue_sql('select count(*) as N from {`ntab`}', .con = ddb))$N)
}