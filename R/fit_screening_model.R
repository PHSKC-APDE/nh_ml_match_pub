#' Fit the first stage screening model
#' @param duck path to duckdb
#' @param fmtab DBI::Id to training data (fitme)
#' @param bounds numeric bounds to which the ML models will focus
#' @param theform formula for the models
#' @param ... for targets orchestration
fit_screening_model = function(duck, fmtab, bounds, theform, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  fitme = DBI::dbGetQuery(ddb,
                          glue::glue_sql(
                            'select * from {`fmtab`}
                            order by sortid'
                            , .con = ddb))
  data.table::setDT(fitme)
  
  l = fit_link(fitme, theform, bounds = bounds, lasso = T, stage_one_only = T)
  
  
}
