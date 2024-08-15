add_nicknames_to_db = function(duck, data_version, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1])
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  nn = read.csv('https://raw.githubusercontent.com/carltonnorthern/nicknames/master/names.csv', header = FALSE)
  setDT(nn)
  nn[, id := .I]
  nn = melt(nn, id.vars = 'id')
  nn = nn[value != '']
  nn = nn[, .(name = clean_name_column(value), name_id = id)]
  nn = nn[, .(name_id = list(unique(name_id))), name]
  DBI::dbWriteTable(ddb, DBI::Id(table = paste0('nicknames_', data_version)), nn, overwrite = T)
  
  return(rlang::hash(nn))
}
