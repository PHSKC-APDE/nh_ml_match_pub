#' NOTE: Most of this functionality is (re)implemented in identify_cutoff.R
#' Refit a stacker model with crossvalidation 
#' @param duck connection to duckdb
#' @param mods stacker
#' @param mversion model version
#' @param test_tab table in duckdb with training data
#' @param ... used for targets orchestration
evaluate_oos_test_data = function(duck, mods, mversion, test_tab, cutme, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = TRUE)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  
  fitme = data.table::setDT(
    dbGetQuery(ddb,glue::glue_sql('Select * from {`test_tab`}', .con = ddb))
  )
  
  if(is.character(mods)){
    if(file.exists(mods)){
      mods = readRDS(mods)
    } else {
      stop('goofy things')
    }
  }
  
  # Predictions
  preds = predict(mods, fitme)
  
  ans = fitme[, .(id1, id2, pair = factor(pair, 0:1, c('No', 'Yes')), oosYes = preds$final)]
  ans[, oosNo := 1-oosYes]
  ans[, c('cv', 'cvYes', 'cvNo') := list(factor(oosYes >=cutme$cutpoint, c(F,T), c('No', "Yes")), oosYes, oosNo)]
  
  # ROC curve
  roc = yardstick::roc_curve(ans, pair, oosNo)
  setDT(roc)
  roc_curve = ggplot(roc, aes(x = 1 - specificity, y = sensitivity)) +
    geom_path() +
    geom_abline(lty = 3) +
    coord_equal() +
    theme_bw() +
    ggtitle(paste0('ROC curve'))
  
  # Find the point of maximum accuracy overall
  maxacc = lapply(roc[.threshold>0 & .threshold<1, .threshold], function(ttt){
    truth = ans$pair
    estimate = factor(ans$estimateYes>ttt, c(F, T), c("No", 'Yes'))
    data.table(.threshold = ttt, accuracy = yardstick::accuracy_vec(truth, estimate))
  })
  maxacc = rbindlist(maxacc)
  oos_cut = maxacc[accuracy == max(maxacc$accuracy)][, mean(.threshold)]
  
  # Out of sample metrics
  ans[, oos := factor(oosYes>=oos_cut, c(F,T), c('No', "Yes"))]
  oos_mets = ans[, yardstick::metrics(data = .SD, truth = pair, estimate = oos, oosNo)]
  setDT(oos_mets)
  
  # Metrics using the CV cutpoint
  cv_mets = ans[, yardstick::metrics(data = .SD, truth = pair, estimate = cv, cvNo)]
  setDT(cv_mets)
  
  list(oos_mets, cv_mets)
}