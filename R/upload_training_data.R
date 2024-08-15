upload_training_data = function(duck, train, test = TRUE, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  con = hhsaw()
  
  ttt = DBI::dbGetQuery(ddb, glue::glue_sql('Select * from {`train[[1]]`}', .con = con))
  tab = DBI::Id(schema = 'noharms', table = as.character(train[[1]]@name))
  DBI::dbWriteTable(con, tab, value = ttt, overwrite = T)
  
  if(test){
    ttt = DBI::dbGetQuery(ddb, glue::glue_sql('Select * from {`train[[3]]`}', .con = con))
    tab = DBI::Id(schema = 'noharms', table = as.character(train[[3]]@name))
    DBI::dbWriteTable(con, tab, value = ttt, overwrite = T)
  }

  
  return(list(tab, nrow(ttt), 1, test))

}
