#' Collapse/aggregate links from the base id level to a slightly more aggregate one.
#' @param comp file path of the components tables
#' @param idtab DBI::Id() of HHSAW table with the ids
#' @param idcol character (length 2). Names of the id columns linking idtab (first value) and comptab(second value)
#' @param by character vector of column names in idtab by which to compute the new components
#' @param model_version model version
#' @param ... used for targets dependency setting
collapse_links = function(comp, idtab, idcol = c('hash_id', 'id'),  by = c('source_id', 'source_system'), model_version, tablename = paste0(model_version, '_components'),..., returnobj = FALSE, con = 'duck', duck){

  if(con == 'hhsaw'){
    con = hhsaw()
  } else{
    con = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  } 
  
  if(!is.data.frame(comp[[1]])){
    compt = readRDS(comp)
    # diags = compt[[3]]
    # icomp_sum = compt[[2]]
    compt = compt[[1]]
  }else{
    compt = comp[[1]]
  }



  lidcol = DBI::Id(table = 'l', column = idcol[1])
  bys = lapply(by, function(y) DBI::Id(table = 'l', column = y))
  q = glue::glue_sql('Select distinct {`lidcol`}, {`bys`*} from {`idtab`} as l', .con = con)
  r = data.table::setDT(DBI::dbGetQuery(con,q))
  
  if(is.character(r[[idcol[1]]]) || is.character(compt[[idcol[2]]])){
    if(!is.character(r[[idcol[[1]]]])) r[, (idcol[1]) := as.character(get(idcol[1]))]
    if(!is.character(r[[idcol[[2]]]])) compt[, (idcol[2]) := as.character(get(idcol[2]))]
  }
  
  r = merge(r, compt, by.x = idcol[1], by.y = idcol[2])
  rm(compt); rm(comp);
  
  r[, sid := paste0(source_system, '|', source_id)]
  
  # components
  comps = lapply(c('clus_id', 'comp_id'), function(cid){
    g = igraph::graph_from_data_frame(unique(r[, .SD, .SDcols = c('sid', cid, by)])[, .SD, .SDcols = c('sid', cid)], directed = F)
    # Find the components
    comp = igraph::components(g)
    comp = data.table(aid = comp$membership, sid = names(comp$membership))
    setnames(comp, 'aid', paste0('aid_', cid))
    comp = comp[sid %in% r$sid]
  })
  comp = merge(comps[[2]], comps[[1]], by = 'sid', all.x = T)
  comp = merge(comp, r, by = 'sid')
  comp[, sid := NULL]
  

  
  # setnames(comp, c('aid', 'cid'),)
  setorder(comp, -source_id)
  
  if(returnobj) return(comp)
  
  upload_table(comp, DBI::Id(schema = 'noharms', table = tablename), overwrite = T)
  # dbWriteTable(con, DBI::Id(schema = 'noharms', table = paste0(model_version, '_components')), value = comp, overwrite = T)
  # dbWriteTable(con, DBI::Id(schema = 'noharms', table = paste0(model_version, '_components_diag_clustered')), value = diags, overwrite = T)
  # dbWriteTable(con, DBI::Id(schema = 'noharms', table = paste0(model_version, '_components_diag_unclustered')), value = icomp_sum, overwrite = T)
  
  return(list(rlang::hash(comp), DBI::Id(schema = 'noharms', table = tablename)))
  
  
}
