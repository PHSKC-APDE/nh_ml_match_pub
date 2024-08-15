#' Generate the first name frequncy table and put it in the duckdb
#' @param duck the output of init_duck, specifically, the file path to the duckdb
create_first_name_freq = function(duck, data_version){
  con = hhsaw()
  ddb = dbConnect(duckdb(), dbdir = duck[1])
  on.exit({
    DBI::dbDisconnect(ddb, shutdown = TRUE)
    DBI::dbDisconnect(con)
  })
  
  fn = DBI::dbGetQuery(con, 'Select first from noharms.identifiers')
  setDT(fn)
  fn[, fn1 := tstrsplit(trimws(firstname), split = ' ', keep = 1, fixed = T)]
  fn[, fn1:= hyrule::clean_names(fn1)]
  
  fnbyid = fn[, .(fn1 = last(fn1)), .(hash_id)]
  fnbynm = fnbyid[, .N, fn1]
  fnbynm[, first_name_freq := round((N - min(N))/(max(N) - min(N)),4)]
  fn = merge(fn, fnbynm, by = 'fn1', all.x = T)
  fnbyid2 = fn[, .(first_name_freq = max(first_name_freq, na.rm = T)), .(hash_id)]
  fnbyid2[, id := hash_id]
  
  DBI::dbWriteTable(ddb, DBI::Id(table = 'fnf'), fnbyid2, overwrite = T)
  
  return(rlang::hash(fnbyid2))
  
  
  
}