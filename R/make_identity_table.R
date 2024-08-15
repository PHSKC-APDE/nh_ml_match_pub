make_identity_table = function(comptable, mversion, aid_col = DBI::Id(column = 'aid_clus_id')){
  con = hhsaw()
  
  q1 = ('drop table if exists noharms.id_linkage')
  q2 = glue::glue_sql("select distinct
        source_id collate Latin1_General_CS_AS as source_id, 
        source_system,
        {`aid_col`} as noharms_id,
        {mversion} as model_version
        into noharms.id_linkage
        from {`comptable`}"
        , .con = con)
  
  DBI::dbExecute(con, q1)
  DBI::dbExecute(con, q2)
  
  # Add ids that are not linked to anyone
  q3 = glue::glue_sql("
    
    insert into noharms.id_linkage
    select
    source_id, 
    source_system,
    ROW_NUMBER() OVER(order by source_system, source_id) * -1 as noharms_id,
    {mversion} as model_version
    from(
      select distinct i.source_id, i.source_system
      from noharms.identifiers as i
      left join noharms.id_linkage as l on i.source_id collate Latin1_General_CS_AS = l.source_id AND i.source_system = l.source_system
      where l.source_system IS NULL
    ) as s
  
        ", .con = con)
  dbGetQuery(con, q3)
  # Add timestamp
  dbGetQuery(con,
             '
             alter table noharms.id_linkage
             add timestamp as GETDATE()
             ')
  # ,
  #       CURRENT_TIMESTAMP as timestamp
  
  # add to the log
  res = DBI::dbGetQuery(con, 'select distinct model_version, timestamp from noharms.id_linkage')
  DBI::dbWriteTable(con, DBI::Id(schema = 'noharms', table = 'log_id_linkage'), value = res, append = T)
  
  return(res)
}