# Libraries ----
library('odbc')
library('glue')
library('data.table')
library('stringdist')
library('targets')
library('hyrule')
library('kcgeocode')
library('stringr')
library('DBI')

# File Paths ----
duckpath = "[FILEPATH REDACTED]ducks/data.duckdb"
data_ver = 'd5' # increment this to indicate a refresh in noharms.identifiers
mod_ver = 'turtle' # version of the models
old_ver = 'rhino'
version = mod_ver
nmfq_version = 1 #increment this to refresh the first name frequency file
duckdir = '[FILEPATH REDACTED]ducks'
outfol = file.path('[FILEPATH REDACTED]', mod_ver)
cifsdir = '[FILEPATH REDACTED]NOHARMS/linkage'

# Load helpers ----
tar_source()

# Set up db tabs ----
con = hhsaw()
tab = DBI::Id(schema = 'noharms', table = version)
otab = DBI::Id(schema = 'noharms', table = old_ver)
trntab = DBI::Id(schema = 'noharms', table = paste0(version, '_fitme'))
tsttab = DBI::Id(schema = 'noharms', table = paste0(version, '_fitme_test'))
ttab = DBI::Id(schema = 'noharms', table = paste0(version, '_fitme_full'))
dbExecute(con, glue_sql('drop table if exists {`ttab`}',.con = con))
dbExecute(con, glue::glue_sql('select * into {`ttab`} from (select * from {`trntab`} union select * from {`tsttab`}) as a', .con = con))

# ddb connection ----
ddb = DBI::dbConnect(duckdb::duckdb(), duckpath)
dtab = DBI::Id(table = paste0('data_', data_ver))
ahtab = DBI::Id(table = paste0('ah_', data_ver))
zhtab = DBI::Id(table = paste0('zh_', data_ver))

# Other helpers ----
cpoint = readRDS(file.path(outfol, paste0('cutoff_metrics_', version,'.rds')))$cutpoint
cur_cut = cpoint
old_cut = readRDS(file.path(dirname(outfol),old_ver, paste0('cutoff_metrics_', old_ver, '.rds')))$cutpoint

# types of training data ----
## toggles ----
data_disagree = F
oldvnew = F
pnearcut = F
blkrules = F
idhcomp = F
cluscomp = F
clusnolink = T
bridges = T
## Where insample or out of sample is wrong ----
if(data_disagree){
r1 = dbGetQuery(con, glue::glue_sql(.con = con,
                    "
                    select t.id1, t.id2, t.pair, r.final
                    from {`ttab`} as t
                    left join 
                    {`tab`} as r on t.id1 = r.id1 AND t.id2 = r.id2
                    where
                    (r.final >= {cpoint} AND pair = 0) OR (r.final < {cpoint} and pair = 1)
                    "))
setDT(r1)
r1[, type := 'wrongprediction']
}else{
  r1 = data.table()
}

if(bridges){
  
  # For low density connections of a particular size, find the bridges
  badnets = dbGetQuery(con, '
  select * from noharms.turtle_components_diag_unclustered where size >=15 and density <= .4  order by [size] desc')
  
  bns = lapply(badnets$comp_id, function(b){
    
    n = retrieve_network_info(ddb, 
                                   con,
                                   net_id_val = b, 
                                   net_id_col = DBI::Id('comp_id'), 
                                   net_tab = DBI::Id(schema = 'noharms', table = paste0(mod_ver, '_components')),
                                   result_tab = tab,
                                   identifier_tab = dtab,
                                   cutpoint = cur_cut)
    
    g = igraph::graph_from_data_frame(n$edges[, .(id1, id2, weight = final)], F, vertices = n$nodes[, .(name = main_id)])
    br = igraph::bridges(g)
    
    return(n$edges[br][final<1])
    
  })
  
  rb = rbindlist(bns)
  rb[, type := 'bridges']
  
}else{
  rb = data.table()
}


## Where new and old version are different ----
if(oldvnew){
  on_n = 100
  q2 = glue::glue_sql('
                select top({on_n}) r.id1, r.id2, r.new, r.old
                from(
                  select coalesce(n.id1,o.id1) as id1, coalesce(n.id2, o.id2) as id2, --n.id1 as nid1, n.id2 as nid2, o.id1 as oid1, o.id2 as oid2,
                    (case when n.final IS NULL then .05
                    else n.final
                    end) as new,
                    (case when o.final IS NULL then .05
                      else o.final
                      end) as old
                  from {`tab`} as n
                  full join {`otab`} as o on n.id1 = o.id1 AND n.id2 = o.id2
                  where (n.final > {cur_cut} OR o.final > {old_cut})
                ) as r
                where
                NOT (r.new > {cur_cut} AND r.old > {old_cut})
                AND NOT (r.new <= {cur_cut} AND r.old <= {old_cut})
                order by abs(new - old) desc
                ', .con = con)
  r_on = setDT(dbGetQuery(con, q2))
  r_on[, abs_dif := abs(new-old)]
  r_on[, type := 'oldnew']
  r_on[, final := new]
}else{
  r_on = data.table()
}


## Cases near the cutpoint ----
if(pnearcut){
  nearcut = 100
  q3 = glue::glue_sql(
    "
    select top({nearcut}) r.id1, r.id2, r.final
    from {`tab`} as r
    where r.final >= ({cur_cut}-.1) AND r.final<=({cur_cut}+.1)
    order by NEWID()
    ", .con = con)
  r_nc = setDT(dbGetQuery(con, q3))
  r_nc[, type := 'nearcut']
}else{
  r_nc = data.table()
}

## Unique block rule contributions ----
if(blkrules){
  n_per_br = 10
  br = readRDS(file.path('[FILEPATH REDACTED]', version, 'ublks.rds'))
  
  r_br = br[, sample(.I, .N)[seq_len(n_per_br)], uid]
  r_br = br[r_br$V1, .(id1, id2, final, type = 'blockrules')]
}else{
  r_br = data.table()
}

## IDH comparison ----
if(idhcomp){
  n_per_bkt = 50
  idh_comp = readRDS(file.path('[FILEPATH REDACTED]',version, paste0('idh_ml_comp_', version, '.rds')))
  idh_comp = idh_comp[paste0(ss1, sid1) != paste0(ss2, sid2)]
  r_idhc = idh_comp[idh_comp[, sample(.I, n_per_bkt), .(ml = !is.na(noharms_id), idh = !is.na(kcmaster_id))]$V1,]
  duckdb::duckdb_register(ddb, 'ridh', r_idhc)
  
  r_idh = dbGetQuery(ddb, glue::glue(
    "
    select l.main_id as id1, r.main_id as id2, b.ss1, b.sid1, b.ss2, b.sid2
    from ridh as b
    left join data_{data_ver} as l on l.source_id = b.sid1 AND l.source_system = b.ss1
    left join data_{data_ver} as r on r.source_id = b.sid2 AND r.source_system = b.ss2
    "
  ))
  
  setDT(r_idh)
  
  # keep only 1 id from each source
  r_idh = r_idh[, .(id1 = first(id1), id2 = first(id2)), .(ss1, sid1, ss2, sid2)]
  r_idh[, type := 'idhcomp'][, final := NA_real_]
  
  
}else{
  r_idh = data.table()
}

## Same big cluster, different 1:1 clusters ----
if(cluscomp){
  n_cc = 100
  r_cc = dbGetQuery(con, glue::glue_sql('select top({n_cc}) l.id1, l.id2, final
                           from {`tab`} as l
                           left join {`ctab`} as c1 on l.id1 = c1.main_id
                           left join {`ctab`} as c2 on l.id2 = c2.main_id
                           where c1.comp_id = c2.comp_id AND c1.clus_id != c2.clus_id
                           and (concat(c1.source_id, c1.source_system) != concat(c2.source_id, c2.source_system))
                           order by NEWID()
                                 ', .con = con))
  setDT(r_cc)
  r_cc[, type := 'clustercompare']
  
  
}else{
  r_cc = data.table()
}

## Same big cluster, no 1:1 link ----
if(clusnolink){
  n_cnl = 100
  
  r_cnl = dbGetQuery(con,
  glue::glue_sql('
        
        with oc as (select cast(main_id as bigint) as main_id, comp_id from {`ctab`}) -- top(10000) 
        
        select top({n_cnl}) l.main_id as id1, r.main_id as id2, t.final
        from oc as l
        inner join oc as r
        on (
          r.comp_id = l.comp_id --AND
          --l.source_id != r.source_id AND
          --l.main_id < r.main_id
        )
        left join {`tab`} as t
        on t.id1 = l.main_id AND t.id2 = r.main_id
        where
        (t.final IS NULL OR t.final < {cur_cut}) AND
        l.main_id < r.main_id
        order by NEWID()
                                         
                                         ', .con = con))
  setDT(r_cnl)
  r_cnl[, c('id1', 'id2') := lapply(.SD, as.numeric), .SDcols = c('id1', 'id2')]
  stopifnot(all(r_cnl[, id1<id2]))
  
  r_cnl[, type := 'impliedlinks']

  
}else{
  r_cnl = data.table()
}

# Combine various parts ----
r = rbind(
  r1, 
  r_on, 
  r_nc,
  r_br,
  r_idh,
  r_cc,
  r_cnl,
  rb,
  fill = T)
setorder(r, type, final)
r = r[, .(type = last(type), final = last(final)), .(id1, id2)]
duckdb::duckdb_register(ddb, 'pairs4eval', r)

## pull the identifiers
d = setDT(
  dbGetQuery(
    ddb,
    glue_sql(
    "
      select distinct data.main_id, first_name_noblank, middle_name_noblank,
      last_name_noblank, ssn, dob, gender, source_system, source_id
      from {`dtab`} as data
      inner join (
       select id1 as main_id from pairs4eval
       union
       select id2 as main_id from pairs4eval
      ) as pairs
      on data.main_id = pairs.main_id
    
    ",
    .con = ddb)
  )
)

## pull address and zip history
ah = setDT(
  dbGetQuery(
    ddb,
    glue_sql(
      "
      select distinct data.main_id, adds.source_id, adds.source_system, geo_hash_geocode,
      from {`ahtab`} as adds
      left join {`dtab`} as data on adds.source_id = data.source_id AND data.source_system = adds.source_system
      inner join (
       select id1 as main_id from pairs4eval
       union
       select id2 as main_id from pairs4eval
      ) as pairs
      on data.main_id = pairs.main_id
    
    ",
      .con = ddb)
  )
)
locs = kcgeocode::fetch_addresses(unique(ah$geo_hash_geocode),input_type = 'geocode', geocode = T, con = con)
setDT(locs)
locs = locs[, .(geo_hash_geocode, geo_add1_clean, geo_city_clean, geo_zip_clean)]
locs[, names(locs) := lapply(.SD, function(x){
  x[is.na(x)] <- ''
  x
})]
locs[, address := paste(geo_add1_clean, geo_city_clean, geo_zip_clean)]
ah = merge(ah[, .(main_id, geo_hash_geocode)], 
           locs[, .(geo_hash_geocode, address)], by = c('geo_hash_geocode'), all.x = T)
ah = unique(ah[, .(main_id, address)])
setorder(ah, main_id, address)
ah[, addnum := paste0('ad', seq_len(.N)), main_id]
ah = dcast(ah, main_id ~ addnum, value.var = 'address')
zh = setDT(
  dbGetQuery(
    ddb,
    glue_sql(
      "
      select distinct data.main_id, zip,
      from {`zhtab`} as adds
      left join {`dtab`} as data on adds.source_id = data.source_id AND data.source_system = adds.source_system
      inner join (
       select id1 as main_id from pairs4eval
       union
       select id2 as main_id from pairs4eval
      ) as pairs
      on data.main_id = pairs.main_id
      order by zip
    
    ",
      .con = ddb)
  )
)
zh[, znum := paste0('z', seq_len(.N)), main_id]
zh = dcast(zh, main_id ~ znum, value.var = 'zip')

d = merge(d, ah[, .(main_id, ad1, ad2, ad3)], all.x = T, by = 'main_id')
d = merge(d, zh[, .(main_id, z1, z2, z3)], all.x  = T, by = 'main_id')

# Save results
r = r[, .(id1, id2, final, type)]
r = r[id1 %in% d[, main_id] & id2 %in% d[, main_id]]

setorder(r, -type, final)
setcolorder(d, 'main_id')

saveRDS(r[, .(id1, id2, final, type)], file.path(cifsdir, 'trainme', 'pairs.rds'))
setnames(d, 'main_id', 'id1')
saveRDS(d, file.path(cifsdir, 'trainme', 'd1.rds'))
setnames(d, 'id1', 'id2')
saveRDS(d, file.path(cifsdir, 'trainme', 'd2.rds'))
duckdb::duckdb_unregister(ddb, 'pairs4eval')
options(shiny.launch.browser = .rs.invokeShinyWindowExternal)
matchmaker(file.path(cifsdir, 'trainme'))

