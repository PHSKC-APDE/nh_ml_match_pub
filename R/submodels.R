make_folds = function(duck, fmtab, screener, stacked = T){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  fitme = DBI::dbGetQuery(ddb,
                          glue::glue_sql(
                            'select * from {`fmtab`}
                            order by sortid'
                          , .con = ddb))
  data.table::setDT(fitme)
  rsample::vfold_cv(fitme[screener$i,], 5)
  
}

fit_submodel = function(duck, fmtab, screener, folds, m, theform, apply_screen = T, stacked = TRUE){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  trainsub = DBI::dbGetQuery(ddb,
                          glue::glue_sql(
                            'select * from {`fmtab`}
                            order by sortid'
                          , .con = ddb))
  data.table::setDT(trainsub)
  
  if(apply_screen) trainsub = trainsub[screener$i]
  
  vvv = attr(terms(theform), 'term.labels')
  
  
  # recipe
  link_rec = recipes::recipe(trainsub, formula = theform)

  params = extract_parameter_set_dials(m) %>% finalize(trainsub[ ,.SD, .SDcols = vvv])
  
  wf = workflow(link_rec, m)
  
  if(stacked){
    ctrl = stacks::control_stack_bayes()
  } else{
    ctrl = control_bayes(save_workflow = T)
  }
  
  wf_search = tune::tune_bayes(wf,
                               resamples = folds,
                               control = ctrl,
                               iter = 100,
                               initial = 15,
                               metrics = yardstick::metric_set(mn_log_loss),
                               param_info = params)
  if(!stacked) wf_search = fit_best(wf_search)
  
  wf_search = butcher::butcher(wf_search)
  
  
}