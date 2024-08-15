#' Identify the cutoff points 
#' @param ... cutoff iterations results
identify_cutoff = combine_cutoffs = function(..., duck, testtab, mods, cat_cut_vars = c('missing_ssn', 'missing_zip', 'missing_ah')){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = TRUE)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  r = rbindlist(list(...))
  findcut = function(obs, pred){
    incorrect = vapply(seq(0,1,.001), function(i) sum(as.numeric(obs) != as.numeric((pred>=i))), 1)
    cutter = which(incorrect ==min(incorrect))
    median(seq(0,1,.001)[cutter])
  }
  
  ovr_cut = r[, findcut(pair, final)]
  cat_cut = r[, .(category = findcut(pair, final)), cat_cut_vars]
  
  r[, overall := ovr_cut]
  r = merge(r, cat_cut, all.x = T, by = cat_cut_vars)
  r[, cat_ans := as.integer(final>=category)]
  r[, ovr_ans := as.integer(final>=overall)]
  
  # get the out of sample results
  if(is.character(mods)){
    if(file.exists(mods)){
      mods = readRDS(mods)
    } else {
      stop('goofy things')
    }
  }
  
  oos = setDT(DBI::dbGetQuery(ddb, glue::glue_sql('Select * from {`testtab`}', .con = ddb)))
  oos[, final := predict(mods, oos)$final]
  oos[, overall := ovr_cut]
  oos = merge(oos, cat_cut, all.x = T, by = cat_cut_vars)
  oos[, cat_ans := as.integer(final>=category)]
  oos[, ovr_ans := as.integer(final>=overall)]
  
  # Compute some fun metrics
  ## Cross validated results
  r[, truth := factor(pair, c(0,1), c('No', 'Yes'))]
  r[, ovr_ans := factor(ovr_ans, c(0, 1), c('No', 'Yes'))]
  r[, cat_ans := factor(cat_ans, c(0, 1), c('No', 'Yes'))]
  r[, ovr_ansYes := final]
  r[, ovr_ansNo := 1-final]
  
  make_mets = yardstick::metric_set(accuracy, kap, mn_log_loss, roc_auc, f_meas)
  mets_ovr = rbind(
    as.data.table(r[, make_mets(.SD, truth, estimate = ovr_ans, ovr_ansNo)])[, cut_type := 'Overall'],
    as.data.table(r[, make_mets(.SD, truth, estimate = cat_ans, ovr_ansNo)])[, cut_type := 'Category']
  )
  

  mets_ovr[, Iteration := 0]
  byround = rbind(
    as.data.table(r[, make_mets(.SD, truth, estimate = ovr_ans, ovr_ansNo), .(Iteration = i)])[, cut_type := 'Overall'],
    as.data.table(r[, make_mets(.SD, truth, estimate = cat_ans, ovr_ansNo), .(Iteration = i)])[, cut_type := 'Category']
  ) 
  cv_mets = rbind(mets_ovr, byround)
  
  ## OOS results
  oos[, truth := factor(pair, c(0,1), c('No', 'Yes'))]
  oos[, ovr_ans := factor(ovr_ans, c(0, 1), c('No', 'Yes'))]
  oos[, cat_ans := factor(cat_ans, c(0, 1), c('No', 'Yes'))]
  oos[, ovr_ansYes := final]
  oos[, ovr_ansNo := 1-final]
  oos_mets = rbind(
    as.data.table(oos[, make_mets(.SD, truth, estimate = ovr_ans, ovr_ansNo)])[, cut_type := 'Overall'],
    as.data.table(oos[, make_mets(.SD, truth, estimate = cat_ans, ovr_ansNo)])[, cut_type := 'Category']
  )

  
  list(cutpoint = ovr_cut,
       cut_tab = cat_cut,
       cv_metrics = cv_mets,
       oos_metrics = oos_mets)
  
}
