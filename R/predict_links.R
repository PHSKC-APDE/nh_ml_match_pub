#' Predict links
#' @param duck path to duckdb
#' @param mods stacking model to predict from
#' @param blktab DBI::Id of the table specifying the pairs to evaluate
#' @param b data.table containing the block id (bid), start and end rids
#' @param ofol directory where outputs should be saved
#' @param ... for targets orchestration. Not used
predict_links = function(duck, mods, blktab, b, ofol, data_version, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  if(is.character(mods)){
    if(file.exists(mods)){
      mods = readRDS(mods)
    } else {
      stop('goofy things')
    }
  }
  
  if(!is.data.frame(b) && is.list(b) && length(b) == 1) b = b[[1]]
  loadspatial(ddb)
  mmf = make_model_frame(
    ddb = ddb,
    ptab = blktab,
    dtab = DBI::Id(table = paste0('data_', data_version)),
    fnftab = DBI::Id(table = paste0('first_name_noblank', '_freq_', data_version)),
    lnftab = DBI::Id(table = paste0('last_name_noblank', '_freq_', data_version)),
    atab = DBI::Id(table = paste0('ah_', data_version)),
    ztab = DBI::Id(table = paste0('zh_', data_version)),
    ssn_tab = DBI::Id(table = paste0('ssnnames_', data_version)),
    ssid_tab = DBI::Id(table = paste0('ssidnames_', data_version)),
    dobftab = DBI::Id(table = paste0('dob', '_freq_', data_version)),
    nn_tab = DBI::Id(table = paste0('nicknames_', data_version)),
    pair = FALSE,
    bid_filter = c(b$start, b$end)
    
  )
  
  
  
  predme = dbGetQuery(ddb, mmf) # glue::glue_sql('SET threads TO 1; {mmf}', .con = ddb)
  setDT(predme)
  vvv = broom::tidy(mods[[1]])$term
  vvv = vvv[-1]
  for(v in vvv){
    if(!v %in% names(predme)) predme[, (v) := 0]
  }
  preds = predict(mods, predme, members = T)
  
  ### Filter ones that are too low ----
  preds = cbind(predme[, .(id1, id2, missing_ssn, missing_zip, missing_ah)], round(preds,5))
  preds = preds[final >.05]
  
  ootf = file.path(ofol, paste0('preds_',blktab@name, '_', b$bid, '.parquet'))
  arrow::write_parquet(preds, ootf)
  
  ootf
}