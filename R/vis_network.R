#' Visualize a network
#' @param nodes a dataframe containing information on the nodes
#' @param edges a data frame containing info on the edges
#' @param tooltip_cols a vector of the columns in nodes to display in the resulting graph
#' @param labels either character vector of the column(s) in nodes to color by OR a (list) network clustering function
#' 
vis_network = function(nodes, edges, tooltip_cols = c('pname', 'dob', 'ssn', 'main_id', 'source_system', 'source_id'), labels = 'nest_id', title = '', return_data = F){
  stopifnot(all(nodes$main_id %in% unique(c(edges$id1, edges$id2))))
  # augment links
  graph = igraph::graph_from_data_frame(edges[, .(id1, id2, weight = final)], F, vertices = nodes[, .(name = main_id)])
  
  gdf = intergraph::asDF(graph)
  gdf$vertexes$intergraph_id = as.numeric(gdf$vertexes$intergraph_id)
  gnet = network::as.network(gdf$edges, directed = F, vertices = gdf$vertexes)
  
  # convert to ggnet
  # Mostly for the vertex locations
  g = GGally::ggnet2(gnet)
  
  # make node tooltips
  ntt = lapply(labels, function(l){
    nodes[, .(name = as.character(main_id), 
             lab = get(l), 
             lab_col = l, 
             text = apply(.SD, 1, function(t) paste(unlist(t), collapse = '<br>'))), .SDcols = tooltip_cols]
  })
  
  ntt = rbindlist(ntt)
  
  # Extract the data frames
  vdat = setDT(g$data)
  vdat = merge(vdat, gdf$vertexes, all.x = T, by.x = 'label', by.y ='intergraph_id')
  vdat = merge(vdat, ntt, all.x = T, by = 'name')
  
  edat = merge(gdf$edges, unique(vdat[, .(V1 = label, X1 = x, Y1 = y)]), all.x = T, by = 'V1')
  setDT(edat)
  edat = merge(edat, unique(vdat[, .(V2 = label, X2 = x, Y2 = y)]), all.x = T, by = 'V2')
  edat[, midX := (X1 + X2)/2]
  edat[, midY := (Y1 + Y2)/2]
  
  g2 = ggplot(vdat) +
    geom_segment(data = edat, aes(x = X1, xend = X2, y = Y1, yend = Y2, color = weight), linewidth = 1) +
    geom_point(aes(x = x, y = y, text = text, fill = factor(lab)), color = 'transparent', shape = 21, size = 9) +
    geom_point(data = edat, aes(x = midX, y = midY, text = weight, color = weight), size = 3) + 
    theme_void() +
    scale_color_distiller(palette = 'YlOrRd', direction = 1, name = 'Weight', limits = c(.3,1)) +
    scale_fill_brewer(name = 'Cluster', type = 'qual') +
    theme(panel.background = element_rect(fill = 'gray10')) +
    ggtitle(title)
    facet_wrap(~lab_col)
  
  
  if(return_data){
    return(list(g2, list(vdat, edat)))
  }
  
  plotly::ggplotly(g2, tooltip = 'text')
  
  
}

#' Retrieve network info
#' @param ddb connection to duckdb
#' @param aw connection to hhsaw
#' @param net_id_val id of the network to pull
#' @param net_id_col id column in net_tab
#' @param net_id_tab table in hhsaw listing the id <-> net id relationship
#' @param cutpoint value to limit links by
#' @param result_tab table in hhsaw with the results
#' @param identifier_tab table in ddb containing identifiers
#' 
# ddb = DBI::dbConnect(duckdb::duckdb(), tar_read(ducky)[1], read_only = T)
# aw = hhsaw()
# net_id_val = 1659
# net_id_col = DBI::Id(column = 'comp_id')
# net_tab = DBI::Id(schema = 'noharms', table = paste0(tar_read(model_version), '_components'))
# result_tab = DBI::Id(schema = 'noharms', table = tar_read(model_version))
# cutpoint = tar_read(cutme)$cutpoint
# identifer_tab = DBI::Id(table = paste0('data_', tar_read(data_version)))
retrieve_network_info = function(ddb, aw, net_id_val, net_id_col, net_tab, result_tab, cutpoint, identifier_tab){
  
  # Start by selecting the ids (e.g. hash ids or whatever) that make up network
  q1 = glue::glue_sql('Select * from
                      {`net_tab`} as nt
                      where {`net_id_col`} = {net_id_val}',.con = aw)
  
  # Retrieve the relevant links
  q2 = glue::glue_sql('
                       select id1, id2, final from {`result_tab`} as r
                       left join (
                                  select distinct main_id from {`net_tab`}
                                  where {`net_id_col`} = {net_id_val}) as l1 on l1.main_id = r.id1
                       left join (
                                  select distinct main_id from {`net_tab`}
                                  where {`net_id_col`} = {net_id_val}) as l2 on l2.main_id = r.id2
                       where
                       l1.main_id IS NOT NULL and
                       l2.main_id IS NOT NULL AND
                       final >= {cutpoint}
                       
                       ',.con = aw)
  
  node_start = setDT(dbGetQuery(aw, q1))
  edge_start = setDT(dbGetQuery(aw, q2))
  
  # add some relevant columns to the node df
  duckdb::duckdb_register(ddb, 'ns', node_start)
  on.exit(duckdb::duckdb_unregister(ddb, 'ns'))
  
  node_end = dbGetQuery(ddb, glue::glue_sql('select l.*, r.first_name_noblank, r.middle_name_noblank, r.last_name_noblank, r.dob, r.ssn from ns as l
                                            left join {`identifier_tab`} as r on l.main_id = r.main_id
                                            ',.con = ddb))
  setDT(node_end)
  
  node_end[, c('first_name_noblank', 'middle_name_noblank', 'last_name_noblank') := lapply(.SD, function(x){
    x[is.na(x)] <- ''
    x
  }), .SDcols = c('first_name_noblank', 'middle_name_noblank', 'last_name_noblank')]
  node_end[, pname := paste(first_name_noblank, middle_name_noblank, last_name_noblank)]
  return(list(nodes = node_end, edges = edge_start))
  
  
}



#' Visualize a network
#' @param id id of the network
#' @param ntab table on hhsaw with the network/components
#' @param idcol DBI::ID() of the column in ntab specifying the id of the network
vis_link_network = function(id, idcol, ntab, rtab, cutpoint, show_identifiers = F, clus_comp = FALSE, edge_tt = FALSE, return_data = F){
  
  con = hhsaw()
  
  # Network basics
  netbase = dbGetQuery(con, glue::glue_sql('select * from {`ntab`} where {`idcol`} = {id}', .con = con))
  setDT(netbase)[, hash_id:= as.integer(hash_id)]
  # The people
  peeps = dbGetQuery(con, glue::glue_sql('Select distinct l.hash_id, firstname as firstname, middlename as middlename, lastname as lastname, dob, ssn
                                         from noharms.identifiers as l
                                         inner join (select distinct hash_id from {`ntab`} where {`idcol`} = {id}) as r
                                         on l.hash_id = r.hash_id', .con = con))
  
  setDT(peeps)
  peeps = merge(peeps, netbase, all.x = T, by = 'hash_id')
  peeps[is.na(firstname), firstname := '']
  peeps[is.na(middlename), middlename := '']
  peeps[is.na(lastname), lastname := '']
  
  # The links
  links = dbGetQuery(con, glue::glue_sql('
                                         select id1, id2, final from {`rtab`} as r
                                         left join (
                                                    select distinct hash_id from {`ntab`}
                                                    where {`idcol`} = {id}) as l1 on l1.hash_id = r.id1
                                         left join (
                                                    select distinct hash_id from {`ntab`}
                                                    where {`idcol`} = {id}) as l2 on l2.hash_id = r.id2
                                         where
                                         l1.hash_id IS NOT NULL and
                                         l2.hash_id IS NOT NULL AND
                                         final >= {cutpoint}
                                         
                                         ',.con = con))
  setDT(links)
  
  # Make sure it all worked
  stopifnot(all(peeps$hash_id %in% unique(c(links$id1, links$id2))))
  
  # add some extra columns
  links = merge(links, netbase[, .(hash_id = as.integer(hash_id), aid_clus_id)], all.x = T, by.x = 'id1', by.y = 'hash_id')
  
  # augment links
  graph = igraph::graph_from_data_frame(links[, .(id1, id2, weight = final, aid_clus_id)],F, vertices = peeps[, .(name = hash_id)])
  
  if(clus_comp){
    clusalgo = c('leiden4')#c('cluster_infomap') #'cluster_fast_greedy', , 'cluster_walktrap', 'cluster_optimal' l'eiden1', 'leiden2', 'leiden3'
    clus = lapply(c(clusalgo), function(f){
      ff = match.fun(f)
      cst = ff(graph)
      coms = communities(cst)
      # sgs = lapply(seq_along(coms), function(i){
      #   nm = coms[[i]]
      #   sg = subgraph(graph, V(graph)[name %in% nm])
      #   vertex_attr(sg, 'aid_clus_id') <- i
      #   vertex_attr(sg, 'algo') <- f
      #   return(sg)
      # })
      sgs = lapply(seq_along(coms), function(i) data.table(nest_id = i, name = (coms[[i]]), algo = f))
      rbindlist(sgs)
    })
    clus = rbindlist(clus, fill = T)
    clus[, algo := gsub('cluster_', '', algo, fixed = T)]
  }else{
    clus = data.table()
  }
  

  gdf = intergraph::asDF(graph)
  gdf$vertexes$intergraph_id = as.numeric(gdf$vertexes$intergraph_id)
  gnet = network::as.network(gdf$edges, directed = F, vertices = gdf$vertexes)
  
  # convert to ggnet
  # Mostly for the vertex locations
  g = GGally::ggnet2(gnet)
  
  # adjust peeps
  apeeps = peeps[, .(name = as.character(hash_id), nest_id = nest_id, 
            text = paste(
              paste('ID:', hash_id, '<br>'),
              paste('Name:', firstname, middlename, lastname), '<br>',
              'DOB:', dob, '<br>',
              'SSN: ', ssn, '<br>',
              paste0('SID: ', source_system, '|', source_id)))]
  if(!show_identifiers) apeeps[, text := name]
  apeeps[, algo := 'leiden']
  if(nrow(clus)>0){
    clus = merge(clus, apeeps[, .(name, text)], all.x = T, by = 'name')
    apeeps = rbind(apeeps, clus)
  }
  
  # Extract the data frames
  vdat = setDT(g$data)
  vdat = merge(vdat, gdf$vertexes, all.x = T, by.x = 'label', by.y ='intergraph_id')
  vdat = merge(vdat, apeeps, all.x = T, by = 'name')
  
  edat = merge(gdf$edges, unique(vdat[, .(V1 = label, X1 = x, Y1 = y)]), all.x = T, by = 'V1')
  setDT(edat)
  edat = merge(edat, unique(vdat[, .(V2 = label, X2 = x, Y2 = y)]), all.x = T, by = 'V2')
  edat[, midX := (X1 + X2)/2]
  edat[, midY := (Y1 + Y2)/2]

  g2 = ggplot(vdat) +
    geom_segment(data = edat, aes(x = X1, xend = X2, y = Y1, yend = Y2, color = weight), linewidth = 1) +
    geom_point(aes(x = x, y = y, text = text, fill = factor(nest_id)), color = 'transparent', shape = 21, size = 9) +
    geom_point(data = edat, aes(x = midX, y = midY, text = weight, color = weight), size = 3) + 
    theme_void() +
    scale_color_distiller(palette = 'YlOrRd', direction = 1, name = 'Weight', limits = c(.3,1)) +
    # scale_fill_brewer(name = 'Cluster', type = 'qual') +
    theme(panel.background = element_rect(fill = 'gray10')) +
    ggtitle(glue::glue('Linkage network for {unname(idcol@name)} = {id}')) +
    facet_wrap(~algo)

  
  if(return_data){
    return(list(g2, list(vdat, edat)))
  }
  
  plotly::ggplotly(g2, tooltip = 'text')

  # 
  # 
  # plot(graph, mark.groups = lapply(unique(peeps$aid_clus_id), function(i) V(graph)[aid_clus_id == i]))
  # 
  # # the graph at source_id/source_system_level
  # sslinks = merge(links, netbase[, .(id1 = as.integer(hash_id), sid1 = paste0(source_system,'|',source_id))], all.x = T, by = 'id1')
  # sslinks = merge(sslinks, netbase[, .(id2 = as.integer(hash_id), sid2 = paste0(source_system,'|',source_id))], all.x = T, by = 'id2')
  # 
  # og = unique(sslinks[, .(sid1,sid2, aid_clus_id)])
  # 
  # ssgrph = igraph::graph_from_data_frame(og[, .(sid1,sid2)],F, vertices = unique(peeps[, .(sid = paste0(source_system, '|', source_id), aid_clus_id)]))
  # plot(ssgrph, mark.groups = lapply(unique(peeps$aid_clus_id), function(i) V(ssgrph)[aid_clus_id == i]))
  # 
}

leiden1 = function(g) cluster_leiden(g, objective_function = 'CPM')
leiden2 = function(g) cluster_leiden(g, objective_function = 'modularity', resolution_parameter = 2)
leiden3 = function(g) cluster_leiden(g, objective_function = 'modularity', resolution_parameter = 3)
leiden4 = function(g) cluster_leiden(g, objective_function = 'modularity', resolution_parameter = 1)