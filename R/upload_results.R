#' Upload results to DB
upload_results = function(..., model_version, suffix = '', clean_start = T, schema = 'noharms'){
  
  fff = unlist(list(...))
  
  stopifnot(all(file.exists(fff)))
  
  # Connection parameters
  con = hhsaw()
  on.exit(DBI::dbDisconnect(con))
  tab = DBI::Id(schema = schema, table = paste0(model_version,suffix))
  
  if(clean_start){
    dropper = glue::glue_sql('drop table if exists {`tab`}', .con = con)
    DBI::dbExecute(con, dropper)
  }
  iter = 0
  for(ff in fff){
    if(iter ==0) DBI::dbWriteTable(con, tab, value=arrow::read_parquet(ff)[0], append = T)
    iter = iter + 1
    r = bcp_load_hhsaw(ff, tab, user = 'dcasey@kingcounty.gov', pass = key_get('hhsaw','dcasey@kingcounty.gov'))
  }
  
  list(DBI::dbGetQuery(con, glue::glue_sql('select CHECKSUM_AGG(checksum(*)) as val from {`tab`}', .con = con))$N, tab)
  
}