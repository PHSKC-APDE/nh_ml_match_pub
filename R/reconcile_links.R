#' Groups 1:1 links into individuals/clusters
#' @param ... things to load
#' @param triggers list of things to trigger for targets
#' @param cutpoint numeric. Value that determines a match
#' @param cluster whether links should be subclustered 
reconcile_links = function(..., cutpoint, model_version, ofol, outfile = paste0('components_', model_version, '.rds'), 
                           recursive = FALSE, savefile = T, triggers = list(), method = 'leiden', min_N = 15, max_density = .4){
  
  dots = list(...)
  
  if(!is.data.frame(dots[[1]]) && !inherits(dots[[1]], 'Id')){
    dots = unlist(dots)
    links = lapply(dots, function(x){
      if(is.character(x) && tools::file_ext(x) == 'rds'){
        r = readRDS(x)
      }
      if(is.character(x) && tools::file_ext(x) == 'parquet'){
        r = arrow::read_parquet(x)
      }
      data.table::setDT(r)
      r = r[final >= cutpoint]
      r
    })
    links = rbindlist(links)[, .(id1, id2, weight = final)]
  }else if(inherits(dots[[1]], 'Id')){
    con = hhsaw()
    on.exit(dbDisconnect(con))
    
    links = setDT(dbGetQuery(con, glue::glue_sql('select id1, id2, final as weight from {`dots[[1]]`} where final>= {cutpoint}', .con = con)))
    
  }else{
    links = dots[[1]][final >= cutpoint, .(id1, id2, weight = final)]
    
  }
  

  
  # Make into an undirected graph
  g = igraph::graph_from_data_frame(links, directed = F)
  g = igraph::simplify(g)
  
  icomp = components(g)
  icomp = data.table(id = names(icomp$membership), comp_id = icomp$membership)
  
  # Initial state
  decomp = igraph::decompose(g)
  d = vapply(decomp, edge_density, .1)
  len = vapply(decomp, length, 1)
  
  icomp_sum = data.table(comp_id = seq_along(d), density = round(d,3), size = len)

  
  # Split up large un-dense clusters
  # TODO: Might need to do something to deal with singletons

  clusterer = function(graph, recursive = FALSE, method = 'leiden'){ #min_V = 4, min_density = .3
    # if(length(V(graph))<= min_V || edge_density(graph)<=min_density) return(graph)
    d = edge_density(graph)
    len = length(graph)
    
    if(!(len>=4 & d <= .8)) return(graph)

    # cg = match.fun(method)(g)
    cg = igraph::cluster_leiden(graph, objective_function = 'modularity', n_iterations = 10)

    if(length(cg)==1) return(graph)

    sgs = lapply(communities(cg), function(nm) subgraph(graph, V(graph)[name %in% nm]))
    
    if(recursive){
      return(lapply(sgs, clusterer, recursive = recursive))
    } else{
      return(sgs)
    }
  }
  
  clusme = which(len>=min_N & d <= max_density)

  reclus = lapply(decomp[clusme], clusterer, recursive = recursive)
  
  # convert to subgraphs
  iter = 0
  while(!all(vapply(reclus, is.igraph, T)) & iter <10){
    reclus = purrr::list_flatten(reclus)
    iter = iter + 1
  }
  
  decomp = append(decomp[-clusme], reclus)
  
  # recompute
  d = vapply(decomp, edge_density, .1)
  len = vapply(decomp, length, 1)
  
  cids = lapply(seq_along(decomp), function(i){
    r = as_data_frame(decomp[[i]], 'vertices')
    r$clus_id = i
    r
  })
  cids = rbindlist(cids)
  data.table::setnames(cids, 'name', 'id')
  cids = merge(cids, icomp, by = 'id')
  
  # create a nested id
  cids[, nest_id := as.numeric(factor(clus_id)), keyby = comp_id]
  
  # new diagnostics
  diags = data.table(clus_id = seq_along(decomp), 
                      density = round(d,3),
                      size = len)
  diags[is.na(density), density := 1] # singletons

  
  if(savefile){
    compfp = file.path(ofol, 
                       outfile)
    saveRDS(list(cids,icomp_sum, diags), compfp)
    
    return(compfp)
  }else{
    return(list(cids, icomp_sum, diags))
  }

  
}
leiden = function(g) igraph::cluster_leiden(g, objective_function = 'modularity', n_iterations = 10)