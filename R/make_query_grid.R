make_query_grid = function(duck, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1])
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE, read_only = TRUE))
  
  splink_rules = c('l.dob = r.dob AND l.last_name_noblank = r.last_name_noblank',
                   'l.dob = r.dob AND l.first_name_noblank = r.first_name_noblank',
                   'l.ssn = r.ssn AND length(l.ssn) > 5 AND length(r.ssn) >5',
                   'l.ssn_last4 = r.ssn_last4 and l.last_name_noblank = r.last_name_noblank',
                   'l.ssn_last4 = r.ssn_last4 and l.first_name_noblank = r.first_name_noblank',
                   'l.ssn_last4 = r.ssn_last4 and l.dob = r.dob',
                   'l.dob = r.dob AND l.fn2l=r.fn2l AND l.ln2l = r.ln2l',
                   'l.first_name_noblank = r.first_name_noblank AND l.last_name_noblank = r.last_name_noblank and l.dob_year = r.dob_year',
                   'l.first_name_noblank = r.first_name_noblank AND l.last_name_noblank = r.last_name_noblank and l.dob_month = r.dob_month and l.dob_day = r.dob_day',
                   # 'l.dob = r.dob AND jaro_winkler_similarity(l.first_name_noblank, r.first_name_noblank) > .75 AND jaro_winkler_similarity(l.last_name_noblank, r.last_name_noblank) >.75',
                   'l.dob = r.dob AND jaccard(l.first_name_noblank, r.first_name_noblank) > .5 AND jaccard(l.last_name_noblank, r.last_name_noblank) >.5',
                   'l.dob = r.dob AND contains(l.last_name_noblank, r.last_name_noblank)',
                   'l.dob = r.dob AND contains(r.last_name_noblank, l.last_name_noblank)',
                   'l.dob = r.dob AND contains(l.first_name_noblank, r.first_name_noblank)',
                   'l.dob = r.dob AND contains(r.first_name_noblank, l.first_name_noblank)',
                   # 'jaro_winkler_similarity(l.first_name_noblank, r.first_name_noblank) > .8 AND 
                   #     jaro_winkler_similarity(l.last_name_noblank, r.last_name_noblank) > .8 and
                   #     abs(l.dob_year - r.dob_year) <= 10 AND
                   #     l.dob_month = r.dob_month AND
                   #     l.dob_day = r.dob_day',
                   'jaro_winkler_similarity(l.first_name_noblank, r.first_name_noblank) > .7 AND 
                       jaro_winkler_similarity(l.last_name_noblank, r.last_name_noblank) > .7 and
                       l.dob_year = r.dob_year AND
                       --l.dob_month = r.dob_month AND
                       l.dob_day = r.dob_day',
                   'jaro_winkler_similarity(l.first_name_noblank, r.first_name_noblank) > .7 AND 
                       jaro_winkler_similarity(l.last_name_noblank, r.last_name_noblank) > .7 and
                       l.dob_year = r.dob_year AND
                       l.dob_month = r.dob_month AND
                       (abs(l.dob_day - r.dob_day) <= 10 OR l.dob_day = 1 OR r.dob_day = 1)'
  )
  
  # l.first_name_noblank != r.first_name_noblank and 
  # l.last_name_noblank != r.last_name_noblank and
  # l.ssn_last4 != r.ssn_last4
  
  # Identify the complex rules
  cplex = lapply(c('contains', 'jaccard', 'jaro'), function(r){
    grep(r, tolower((splink_rules)), fixed = T)
  })
  cplex = unique(unlist(cplex))
  simple = splink_rules[-cplex]
  
  splink_rules = data.frame(rrr = c(simple, splink_rules[cplex]), type = 'splink')
  
  simple = paste0('coalesce((', simple, '), false)')
  subsetter = paste(simple, collapse = ' OR ')
  subsetter =  paste0('AND NOT (', subsetter, ')')

  match_rules = list(splink_rules)
  
  grids = lapply(match_rules, function(y) setnames(y, setdiff(names(y), 'type'), letters[seq_len(ncol(y)-1)]))
  grids = rbindlist(grids, fill = T)
  
  grids[, qid := .I]
  grids = melt(grids, c('qid', 'type'))
  grids = grids[!is.na(value)]
  
  qgrid = grids[, glue::glue_sql_collapse(value, sep = ' AND '), .(qid,type)]
  qgrid[, V1:= glue_sql('({V1})', .con = con)]
  
  qgrid[, where := SQL('')]
  qgrid[qid %in% cplex, where := DBI::SQL(subsetter)]
  
  
  qgrid
}