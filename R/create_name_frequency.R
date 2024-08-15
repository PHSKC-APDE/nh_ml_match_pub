#' Generate the first name frequncy table and put it in the duckdb
#' @param duck the output of init_duck, specifically, the file path to the duckdb
create_name_frequency = function(duck, data_version, ...){
  ddb = dbConnect(duckdb(), dbdir = duck[1])
  on.exit({
    DBI::dbDisconnect(ddb, shutdown = TRUE)
  })
  
  ans = lapply(c('first_name_noblank','last_name_noblank', 'dob'), function(nm){
    
    dtab = DBI::Id(table = paste0('data_',data_version))
    tab = DBI::Id(column = nm)
    q = glue::glue_sql('select count(*) as N, {`tab`} from {`dtab`} group by {`tab`}', .con = ddb)
    r = setDT(dbGetQuery(ddb, q))
    r[, paste0(nm, '_freq') := round((N-min(N)) / (max(N) - min(N)),4)]
    DBI::dbWriteTable(ddb, DBI::Id(table = paste0(nm, '_freq_', data_version)), r, overwrite = T)
    
    rlang::hash(r)
    
  })

  return(ans)
}