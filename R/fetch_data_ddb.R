#' Fetch data from a duckdb
#' @param ddb connection to ddb
#' @param source_tab DBI::Id -- the source data table
#' @param ptab table specifying the IDs to pull (DBI::Id)
#' @param dtab table specifying the data table within duckdb
#' @param source_id one of 'main_id" or 'ssid' (short for source_system and source_id)
fetch_data_ddb = function(ddb, source_tab, ptab, dtab, source_id = c('main_id', 'ssid')){

  
  
  stopifnot(!missing(ptab))
  
  scols = names(dbGetQuery(ddb,
                           glue::glue_sql('select * from {`source_tab`} limit 0', .con = ddb)))
  scols = lapply(scols, function(i) DBI::Id(table = 's', column = i))
  
  if(source_id == 'ssid'){
    djoin = glue::glue_sql('left join {`dtab`} as d on d.source_id = s.source_id AND d.source_system = s.source_system', .con = ddb)
    pjoin = glue::glue_sql('on d.main_id = p.id', .con = ddb)
  }else{
    djoin =SQL('')
    pjoin = glue::glue_sql('on s.main_id = p.id', .con = ddb)
  }
  
  r = DBI::dbGetQuery(ddb,
                 glue::glue_sql(
                   "
                   Select {`scols`*} from {`source_tab`} as s
                   {djoin}
                   inner join {`ptab`} as p
                   {`pjoin`}"
                 ,.con = ddb))
  data.table::setDT(r)
  
  return(r)
}
