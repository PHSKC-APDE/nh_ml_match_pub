#' Create a modelling ready dataframe
#' @param duck connection to duckdb
#' @param ptab DBI::Id of the tab containing the pairs to assess
#' @param dtab DBI::Id of the tab containing the data/demographics
#' @param fnftab DBI::Id of the first name frequency table
#' @param lnftab DBI::Id of the last name frequency table
#' @param atab DBI::Id of the tab containing the address history
#' @param ztab DBI::Id of the tab containing the ZIP code history
#' @param ssn_tab DBI::Id of the tab containing the names by ssn
#' @param ssid_tab DBI::Id of the tab containing names by source_system and source id
#' @param dobftab DBI::Id of the tab containing dob by frequency table
#' @param pair logical. Should the pair column be requested?
#' @param bid_filter numeric. lower and upper of the bid column that should be returned
#' @param nn_tab DBI::Id of the tab containing nicknames
# ptab = DBI::Id(table = 'test_train')
# dtab = DBI::Id(table = 'data_test')
# atab = DBI::Id(table = 'ah_test')
# ztab = DBI::Id(table = 'zh_test')
# fnftab = DBI::Id(table = 'first_name_noblank_freq')
# lnftab = DBI::Id(table = 'last_name_noblank_freq')

make_model_frame = function(ddb, ptab, dtab, fnftab, lnftab, atab, ztab, ssn_tab, ssid_tab, dobftab, nn_tab, pair = T, bid_filter = NULL){
  loadspatial(ddb)
  dcols = names(dbGetQuery(ddb, glue::glue_sql('select * from {`dtab`} limit 0', .con = ddb)))
  # l = lapply(setdiff(dcols, 'main_id'), function(x) DBI::Id(table = 'l', column = x))
  # r = lapply(setdiff(dcols, 'main_id'), function(x) DBI::Id(table = 'l', column = x))

  if(pair){
    psql = SQL(',p.pair')
  }else{
    psql = SQL('')
  }
  
  if(missing(bid_filter)){
    bf = SQL('')
    bf2 = bf = SQL('')
  }else{
    bf = glue::glue_sql('AND p.bid>={bid_filter[1]} AND p.bid < {bid_filter[2]}', .con = ddb)
    bf2 = glue::glue_sql('where p.bid>={bid_filter[1]} AND p.bid < {bid_filter[2]}', .con = ddb)
    
  }

  
  # Pull main demographics
  {
  q_d = glue::glue_sql("
     Select 
      p.id1
     ,p.id2
     {psql}
     
     -- date of birth comparisons
     ,IF(l.dob = r.dob, 1, 0) as dob_exact
     ,IF(l.dob_clean_year = r.dob_clean_year, 1, 0) as dob_year_exact
     ,least(hamming(substr(cast(l.dob as varchar), 6, 5), substr(cast(r.dob as varchar), 6, 5))/4, 1) as dob_mdham
     ,case
        when (month(l.dob) = 1 and day(l.dob) = 1 AND month(r.dob) = 1 AND day(r.dob) = 1) then 2
        when (month(l.dob) = 1 and day(l.dob)) then 1
        when (month(r.dob) = 1 AND day(r.dob) = 1) then 1
        else 0 end as dob0101

     -- Sex/gender
     ,IF(l.gender = r.gender, 1, 0) as gender_agree
     
     -- Name  stuff
     ,least(1-jaro_winkler_similarity(r.first_name_noblank, l.first_name_noblank),1) as first_name_jw
     ,least(1-jaro_winkler_similarity(r.last_name_noblank, l.last_name_noblank), 1) as last_name_jw
     ,least(
        1 - jaro_winkler_similarity(r.first_name_noblank, l.last_name_noblank),
        1 - jaro_winkler_similarity(r.last_name_noblank, l.first_name_noblank),
        1
      ) as name_swap_jw
     ,IF(l.middle_initial = r.middle_initial, 1, 0) as middle_initial_agree
     
     --case when a>b then dl/len(a)
     --when a<=b then dl/len(b)
     ,least(
        damerau_levenshtein(
          concat(l.first_name_noblank, l.middle_name_noblank, l.last_name_noblank),
          concat(r.first_name_noblank, r.middle_name_noblank, r.last_name_noblank)
        )/greatest(
          len(concat(l.first_name_noblank, l.middle_name_noblank, l.last_name_noblank)),
          len(concat(r.first_name_noblank, r.middle_name_noblank, r.last_name_noblank))
        ),
        damerau_levenshtein(
          concat(l.first_name_noblank, l.last_name_noblank),
          concat(r.first_name_noblank, r.last_name_noblank)
        )/greatest(
          len(concat(l.first_name_noblank, l.last_name_noblank)),
          len(concat(r.first_name_noblank, r.last_name_noblank))
        )
     ) as complete_name_dl
     
     
     ,if(contains(l.last_name_noblank, r.last_name_noblank) OR contains(r.last_name_noblank, l.first_name_noblank), 1, 0) as last_in_last
     ,if(l.first_name_noblank = r.middle_name_noblank OR r.first_name_noblank = l.middle_name_noblank, 1, 0) as first_is_middle
     
     -- Social security stuff
     ,if(len(l.ssn) >=7 AND len(r.ssn)>=7 AND l.ssn = r.ssn, 1, 0) as ssn_full_exact
     ,if(right(l.ssn,4) = left(r.ssn,4), 1, 0) as ssn_last4_exact
     ,if(l.ssn IS NULL or r.ssn IS NULL, 1, damerau_levenshtein(right(l.ssn,4), right(r.ssn,4))/4) as ssn_last4_dl
     ,if(l.ssn IS NULL or r.ssn IS NULL, 1, 0) as missing_ssn

     -- Data system
     ,if(l.source_system = r.source_system, 1, 0) as same_datasystem

     -- Name frequencies
     ,greatest(fnf1.first_name_noblank_freq, fnf2.first_name_noblank_freq, 0) AS first_name_freq
     ,greatest(lnf1.last_name_noblank_freq, lnf2.last_name_noblank_freq, 0) as last_name_freq
     ,greatest(df1.dob_freq, df2.dob_freq, 0) AS dob_freq
     
     -- Nickname matches
     ,If(list_has_any(nn1.name_id,nn2.name_id) OR list_has_any(nn1.name_id, nn2.name_id), 1, 0) as nickname_match

     from {`ptab`} as p
     left join {`dtab`} as l on p.id1 = l.main_id
     left join {`dtab`} as r on p.id2 = r.main_id
     left join {`fnftab`} as fnf1 on l.first_name_noblank = fnf1.first_name_noblank
     left join {`fnftab`} as fnf2 on r.first_name_noblank = fnf2.first_name_noblank
     left join {`lnftab`} as lnf1 on l.last_name_noblank = lnf1.last_name_noblank
     left join {`lnftab`} as lnf2 on r.last_name_noblank = lnf2.last_name_noblank
     left join {`dobftab`} as df1 on l.dob = df1.dob
     left join {`dobftab`} as df2 on r.dob = df2.dob
     left join {`nn_tab`} as nn1 on l.first_name_noblank = nn1.name
     left join {`nn_tab`} as nn2 on r.first_name_noblank = nn2.name
     where l.main_id IS NOT NULL AND r.main_id is not null
     {bf}
                      
     ", .con = ddb)
  }
  
  # ZIP history
  # result is in survey feet apparently
  q_z = glue::glue_sql("
      select 
      p.id1
     ,p.id2
     ,min(st_distance(z1.geom, z2.geom)) * 1200/3937 / 1000000 as zh_min_distance -- to mega meter
     from {`ptab`} as p
     left join hash_hist as h1 on p.id1 = h1.cid
     left join hash_hist as h2 on p.id2 = h2.cid
     left join {`ztab`} as a1 on h1.source_id = a1.source_id AND h1.source_system = a1.source_system
     left join {`ztab`} as a2 on h2.source_id = a2.source_id AND h2.source_system = a2.source_system
     left join zip_centriods as z1 on a1.zip = z1.zip
     left join zip_centriods as z2 on a2.zip = z2.zip
     where z1.geom IS NOT NULL and z2.geom IS NOT NULL
                       {bf}
     group by p.id1, p.id2                  
                        ", .con = ddb)
  # Address history
  q_a = glue::glue_sql("
     select 
      p.id1
     ,p.id2
     ,min(st_distance(st_point(a1.X, a1.Y), st_point(a2.X, a2.Y))) * 1200/3937 as ah_min_distance -- to meter
     from {`ptab`} as p
     left join hash_hist as h1 on p.id1 = h1.cid
     left join hash_hist as h2 on p.id2 = h2.cid
     left join {`atab`} as a1 on h1.source_id = a1.source_id AND h1.source_system = a1.source_system
     left join {`atab`} as a2 on h2.source_id = a2.source_id AND h2.source_system = a2.source_system
     where a1.X IS NOT NULL and a2.X IS NOT NULL
                       {bf}
     group by p.id1, p.id2
                       ", .con = ddb)
  
  # ssn name lists
  q_ssn = glue::glue_sql(
    "
    select 
      p.id1
     ,p.id2
     ,if(
        (
          list_has_any(a1.ssn_first_name_list, a2.ssn_first_name_list) 
          OR
          list_has_any(list_value(l.first_name_noblank), a2.ssn_first_name_list)
          OR
          list_has_any(a1.ssn_first_name_list, list_value(r.first_name_noblank))
        )
        AND 
        NOT l.source_system IN ('KC_JAIL', 'MUNI_JAIL') AND
        NOT r.source_system IN ('KC_JAIL', 'MUNI_JAIL'), 1, 0) as ssn_any_firstname
     ,if(
     (
      list_has_any(a1.ssn_last_name_list, a2.ssn_last_name_list) 
      OR
      list_has_any(list_value(l.last_name_noblank), a2.ssn_last_name_list) 
      OR
      list_has_any(a1.ssn_last_name_list, list_value(r.last_name_noblank)) 
     )
     AND 
        l.source_system NOT IN ('KC_JAIL', 'MUNI_JAIL') AND
        r.source_system NOT IN ('KC_JAIL', 'MUNI_JAIL'), 1, 0) as ssn_any_lastname
     from {`ptab`} as p
     left join {`dtab`} as l on p.id1 = l.main_id
     left join {`dtab`} as r on p.id2 = r.main_id
     left join {`ssn_tab`} as a1 on l.ssn = a1.ssn
     left join {`ssn_tab`} as a2 on r.ssn = a2.ssn
    {bf2}
    "
  ,.con = ddb)
  
  # source system source_id name lsits
  q_ssid = glue::glue_sql("
     select 
      p.id1
     ,p.id2
     ,if(list_has_any(a1.ssid_first_name_list, a2.ssid_first_name_list), 1, 0) as ssid_any_firstname
     ,if(list_has_any(a1.ssid_last_name_list, a2.ssid_last_name_list), 1, 0) as ssid_any_lastname
     from {`ptab`} as p
     left join {`dtab`} as l on p.id1 = l.main_id
     left join {`dtab`} as r on p.id2 = r.main_id
     left join {`ssid_tab`} as a1 on l.sssi_id = a1.sssi_id
     left join {`ssid_tab`} as a2 on r.sssi_id = a2.sssi_id
     {bf2}

                       ", .con = ddb)
  
  
  q_ans = glue::glue_sql("
                 with demo as ({q_d}),
                 zip as ({q_z}),
                 ads as ({q_a}),
                 ssnnm as ({q_ssn}),
                 ssidnm as ({q_ssid})
                 
                 select
                 dt.* 
                 ,least(zt.zh_min_distance, .1) as mn_ziphist_distance
                 ,if(at.ah_min_distance <10, 1 , 0) as exact_location
                 ,if(zt.zh_min_distance IS NULL, 1, 0) as missing_zip
                 ,if(at.ah_min_distance IS NULL, 1, 0) as missing_ah
                 ,ssn_any_firstname
                 ,ssn_any_lastname
                 ,ssid_any_firstname
                 ,ssid_any_lastname
                 from demo as dt
                 left join zip as zt on dt.id1 = zt.id1 AND dt.id2 = zt.id2
                 left join ads as at on dt.id1 = at.id1 AND dt.id2 = at.id2
                 left join ssnnm as s1 on dt.id1 = s1.id1 AND dt.id2 = s1.id2
                 left join ssidnm as si on dt.id1 =si.id1 AND dt.id2 = si.id2;
                ", .con = ddb)
  
}
# 

