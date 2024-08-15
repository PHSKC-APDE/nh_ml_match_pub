#' Generate a list of start and ends 
#' @param duck file path to duckdb
#' @param blktab table within the duckdb containing the blockign details
#' @param ... additional options used to help targets organize triggers and invalidations
create_bids = function(duck, blktab = DBI::Id(table = 'within'), chksize = 4e6, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = TRUE)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  blks = DBI::dbGetQuery(ddb, glue::glue_sql('select count(*) as N from {`blktab`}', .con = ddb))
  blkseq = c(seq(0, blks$N, chksize))
  blkseq = data.table::data.table(start = blkseq)[, end := start + chksize]
  blkseq[, bid := .I]
  blkseq = split(blkseq, by = 'bid')
  
  return(blkseq)
}