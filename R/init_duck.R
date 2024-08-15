#' Initialize a duckdb 
#' @param duckpath file path to the data.duckdb
#' @param data_version version of the data
init_duck = function(duckpath, data_version = 'test'){
  
  # Load data
  d = clean_noharms_ids()
  ahzh = make_address_history()
  ah = ahzh[[2]]
  zh = ahzh[[1]]
  
  # Create/connect to duckdb
  stopifnot(file.exists(duckpath))
  dpath = duckpath
  ddb = dbConnect(duckdb(), dbdir = dpath)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  # Write files
  ## data
  dtab = DBI::Id(table = paste0('data_', data_version))
  DBI::dbWriteTable(ddb, dtab, d, overwrite = T)
  
  ## add main id to the data table
  dbExecute(ddb,
            glue::glue_sql(
            "insert into hash_hist
            select distinct nextval('seq_cid') as cid, d.source_system, d.source_id, d.clean_hash from {`dtab`} as d
            left join hash_hist as h on d.clean_hash =  h.clean_hash
            where h.clean_hash is NULL",
            .con = ddb))
  dbExecute(ddb, glue::glue_sql('alter table {`dtab`} add column main_id integer', .con = ddb))
  dbExecute(ddb, 
            glue::glue_sql(" update {`dtab`} 
                           set main_id = (
                            select cid from hash_hist 
                            where {`dtab`}.clean_hash = hash_hist.clean_hash
                           );", .con = ddb))
  
  # Create a id field for source_system and source_ids
  ## Create the table
  ssid_tab = DBI::Id(table = paste0('sssi_', data_version))
  dbExecute(ddb, glue::glue_sql('create or replace table {`ssid_tab`} as (
                                select distinct source_id, source_system from {`dtab`}
                                )',.con = ddb))
  ## Row IDs
  dbExecute(
    ddb,
    glue::glue_sql(
      "
         DROP SEQUENCE if exists ss_seq CASCADE;
         CREATE SEQUENCE if not exists ss_seq START 1;
         alter table {`ssid_tab`} add column sssi_id bigint default nextval('ss_seq');
         ",
      .con = ddb
    )
  )
  
  ## Add to the main data
  dbExecute(ddb, glue::glue_sql('alter table {`dtab`} add column sssi_id integer', .con = ddb))
  dbExecute(ddb, 
            glue::glue_sql(" update {`dtab`} 
                           set sssi_id = (
                            select sssi_id from {`ssid_tab`} 
                            where {`dtab`}.source_system = {`ssid_tab`}.source_system AND {`dtab`}.source_id = {`ssid_tab`}.source_id
                           );", .con = ddb))
  
  
  ## Address history
  ahxy = sf::st_coordinates(ah)
  ahxy = cbind(ah[, c('geo_hash_geocode', 'source_id', 'source_system', 'geo_add_type_grade'), drop = T], ahxy)
  data.table::setorder(ahxy, source_id, source_system, geo_hash_geocode)
  
  atab = DBI::Id(table = paste0('ah_', data_version))
  DBI::dbWriteTable(ddb, atab, ahxy, overwrite = T)
  
  ## zip history
  data.table::setorder(zh, source_system, source_id, zip)
  ztab = DBI::Id(table = paste0('zh_', data_version))
  DBI::dbWriteTable(ddb, ztab, zh[!is.na(zip)], overwrite = T)
  
  
  
  ## return a hash of the data
  return(c(dpath, rlang::hash(d), rlang::hash(ahxy), rlang::hash(zh)))
  
}
