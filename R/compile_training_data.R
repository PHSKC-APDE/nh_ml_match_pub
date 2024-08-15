#' Creates a training data set and saves it to disk
#' @param input list of rds or csv files containing the training data
#' @param duck file path to duck db
#' @param model_version model_version
#' @param theform the formula
#' @param test_frac fraciton of data to be held back as test data
#' @param data_version data version
#' @param ... things to help target orchestration
compile_training_data = function(input, duck, model_version, theform, test_frac = 0, data_version, ...){
  
  # Load the pairs
  train = lapply(input, function(f){
    fe = tools::file_ext(f)
    if(fe == 'rds'){
      return(readRDS(f))
    } else if (fe == 'csv'){
      return(data.table::fread(f))
    } else{
      stop(paste(fe, 'is an invalid file format'))
    }
  })
  
  train = data.table::rbindlist(train, fill = T)[, .(id1, id2, pair)]
  # Make sure id1 < id2
  train[id1>id2, c('id1', 'id2') := list(id2, id1)]
  train = train[, .(pair = last(pair)), .(id1,id2)]
  train <- train[pair %in% c(0,1),]
  stopifnot(!anyNA(train))
  train <- unique(train)
  train = train[id1 != id2]

  stopifnot(nrow(train) == nrow(unique(train[, .(id1, id2)])))
  
  # Connect to duckdb database and fetch the relevant people
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1])
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  DBI::dbWriteTable(ddb, name = DBI::Id(table = paste0(model_version, '_train')), value = train, overwrite = T)
  
  loadspatial(ddb)
  
  mmf = make_model_frame(
    ddb = ddb,
    ptab = DBI::Id(table = paste0(model_version, '_train')),
    dtab = DBI::Id(table = paste0('data_', data_version)),
    fnftab = DBI::Id(table = paste0('first_name_noblank', '_freq_', data_version)),
    lnftab = DBI::Id(table = paste0('last_name_noblank', '_freq_', data_version)),
    atab = DBI::Id(table = paste0('ah_', data_version)),
    ztab = DBI::Id(table = paste0('zh_', data_version)),
    ssn_tab = DBI::Id(table = paste0('ssnnames_', data_version)),
    ssid_tab = DBI::Id(table = paste0('ssidnames_', data_version)),
    dobftab = DBI::Id(table = paste0('dob', '_freq_', data_version)),
    nn_tab = DBI::Id(table = paste0('nicknames_', data_version))
    
    
  )
  fitme = data.table::setDT(DBI::dbGetQuery(ddb, mmf))
  stopifnot(nrow(fitme) == nrow(unique(fitme[, .(id1, id2)])))
  
  # Find the number of training pairs that don't have a hash in r
  droppers = nrow(train) -  nrow(fitme)
  
  if((droppers)/nrow(train) >.07) stop(paste0(round((droppers)/nrow(train)*100),'% of labelled pairs have no corrosponding data'))
  
  fitme[, sortid := .I]
  
  # Drop rows with missing
  vvv = attr(terms(theform), 'term.labels')
  start = nrow(fitme)
  fitme = na.omit(fitme, cols = vvv)
  frac = nrow(fitme)/start
  if(nrow(fitme) != start) warning(paste(start - nrow(fitme), 'rows dropped'))
  if(frac<.75 || frac>1) stop(paste0(round(frac,2), ' is the proportion of final rows relative to starting rows'))
  
  fitme[, pair := factor(pair, 0:1, as.character(0:1))]
  
  if(test_frac>0){
    idx = sample(seq_len(nrow(fitme)),size = floor(test_frac * nrow(fitme)))
    test_fitme = fitme[idx,]
    fitme = fitme[-idx]
  }
  
  dbWriteTable(ddb, DBI::Id(table = paste0(model_version,'_fitme')), value = fitme, overwrite = T)
  dbWriteTable(ddb, DBI::Id(table = paste0(model_version,'_fitme_test')), value = test_fitme, overwrite = T)
  
  
  return(list(DBI::Id(table = paste0(model_version, '_fitme')), 
              model_version, 
              DBI::Id(table = paste0(model_version, '_fitme_test')), 
              rlang::hash(train),
              rlang::hash(fitme),
              droppers))
}