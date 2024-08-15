upload_hash_hist = function(duck, data_version, ...){
  ddb = dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
  on.exit({
    DBI::dbDisconnect(ddb, shutdown = TRUE)
  })
  
  con = hhsaw()
  tgt = DBI::Id(schema = 'noharms', table = paste0('hash_hist_',data_version))
  dtab = DBI::Id(table = paste0('data_', data_version))
  a = dbGetQuery(ddb, glue::glue_sql('select distinct h.cid, h.source_system, h.source_id from hash_hist as h 
                                     inner join (select distinct main_id from {`dtab`}) as d on h.cid = d.main_id', .con = ddb))

  upload_table(a, tgt)
  
  return(list(rlang::hash(a), tgt))
  
}