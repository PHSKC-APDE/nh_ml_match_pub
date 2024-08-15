#' linkages that are a-priori decided 
#' @param duck path to duckdb
#' @param mods stacking model to predict from
#' @param fixed_vars character. variables in data that determine a fixed linkage
#' @param ofol directory where outputs should be saved
#' @param data_version
#' @param ... for targets orchestration. Not used
fixed_links = function(duck, mods, fixed_vars = c('source_system', 'source_id'), ofol, data_version, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  if(is.character(mods)){
    if(file.exists(mods)){
      mods = readRDS(mods)
    } else {
      stop('goofy things')
    }
  }
  dtab = DBI::Id(table = paste0('data_', data_version))
  cols = lapply(c('main_id', fixed_vars), function(x) DBI::Id(column = x))
  
  fixins = setDT(dbGetQuery(ddb,
  glue::glue_sql('
                 with d as (select distinct {`cols`*} from {`dtab`}
                 where dob IS NOT NULL)
                 
                 select d1.main_id as id1, d2.main_id as id2
                 from d as d1
                 inner join d as d2 on (
                 d1.source_system = d2.source_system AND 
                 d1.source_id = d2.source_id AND
                 d1.main_id < d2.main_id);
                 
                 ',.con = ddb)))
  
  # Keep only where there are more than 1 id per fixed
  fixins[, screen := 1]
  addme = broom::tidy(mods$stack$coefs)
  setDT(addme)
  if(nrow(addme)>0){
    smods = addme[estimate != 0 & term != '(Intercept)', gsub('.pred_1_', '', term, fixed = T)]
  }else{
    smods = NULL
  }
  cvars = c('missing_ssn', 'missing_zip', 'missing_ah')
  fixins[, c('ens', smods, cvars) := NA_real_]
  fixins[, final := 1]
  fixins[id1>id2, c('id1', 'id2') := list(id2, id1)]
  
  ootf = file.path(ofol, paste0('preds_fixed.parquet'))
  arrow::write_parquet(fixins, ootf)
  
  ootf
}


#' linkages that are a-priori decided between EMS AND RHINO
#' @param duck path to duckdb
#' @param mods stacking model to predict from
#' @param up_table DBI::Id for the table to update/add rows too
#' @param ... for targets orchestration. Not used
ems_rhino_fixed = function(duck, mods, up_table, data_version, ...){
  
  con = hhsaw()
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
  on.exit({
    DBI::dbDisconnect(ddb, shutdown = TRUE)
    DBI::dbDisconnect(con)
    })
    dtab = DBI::Id(table = paste0('data_', data_version))

  ems_rhino = setDT(dbGetQuery(con,"
    WITH EMS AS (
    SELECT DISTINCT
        id.dedup_id,
        PPL.firstname,
        PPL.lastname,
        EVENTS.event_date,
        PPL.source_system,
        LINK.noharms_id
    FROM (
        SELECT 
            source_id collate Latin1_General_CS_AS as source_id,
            event_date,
            source_system
        FROM noharms.stage_outcomes
        WHERE source_system = 'EMS'
        AND event_type = 'Suicide attempt'
        UNION
        SELECT 
            source_id collate Latin1_General_CS_AS as source_id,
            start_date AS event_date,
            source_system
        FROM noharms.stage_other_events
        WHERE source_system = 'EMS'
    ) AS EVENTS
       LEFT JOIN NOHARMS.ems_ids AS ID
              ON ID.RespondingAgency_PCRNumber collate Latin1_General_CS_AS = EVENTS.source_id
    LEFT JOIN NOHARMS.identifiers AS PPL
        ON id.dedup_id collate Latin1_General_CS_AS = PPL.source_id
    LEFT JOIN NOHARMS.id_linkage AS LINK
        ON PPL.source_id collate Latin1_General_CS_AS = LINK.source_id collate Latin1_General_CS_AS
        AND PPL.source_system = LINK.source_system
    AND EVENTS.source_id is not null
    AND PPL.source_system = 'EMS'
),
 
 
-- RHINO
 
RHINO AS (
    SELECT DISTINCT
        EVENTS.source_id AS C_Unique_Patient_ID,
        PPL.firstname,
        PPL.lastname,
        EVENTS.event_date AS Admit_Date_Time,
        PPL.source_system,
        LINK.noharms_id
    FROM (
        SELECT 
            source_id  collate Latin1_General_CS_AS as source_id,
            event_type,
            event_subtype,
            event_date,
            source_system
        FROM noharms.stage_outcomes
        WHERE source_system = 'RHINO'
        AND year(event_date) >= 2019
        AND year(event_date) <= 2022
 
        UNION
 
        SELECT 
            source_id  collate Latin1_General_CS_AS as source_id,
            event_type,
            event_subtype,
            end_date AS event_date,
            source_system
        FROM noharms.stage_other_events
        WHERE source_system = 'RHINO'
        AND year(end_date) >= 2019
        AND year(end_date) <= 2022
    ) AS EVENTS
    RIGHT JOIN NOHARMS.identifiers AS PPL
        ON EVENTS.source_id COLLATE Latin1_General_CS_AS = PPL.source_id
    LEFT JOIN NOHARMS.id_linkage AS LINK
        ON PPL.source_id collate Latin1_General_CS_AS = LINK.source_id collate Latin1_General_CS_AS
        AND PPL.source_system = LINK.source_system
    WHERE PPL.firstname IS NOT NULL
    AND PPL.source_id IS NOT NULL
    AND PPL.source_system = 'RHINO'
)
 

      SELECT ems.dedup_id as source_id1, ems.source_system as source_system1,
    rhino.C_Unique_Patient_ID as source_id2, rhino.source_system as source_system2
    FROM EMS
    INNER JOIN RHINO
    ON EMS.firstname = RHINO.firstname
    AND EMS.lastname = RHINO.lastname
    AND ABS(datediff(day, EMS.event_date, RHINO.Admit_Date_Time)) <= 1;

                               "))
  
    ids = setDT(
      dbGetQuery(ddb, 
                 glue::glue_sql(
                   "select main_id, source_system, source_id from {`dtab`} where source_system in ('RHINO', 'EMS') and dob IS NOT NULL",.con = ddb)))
    
    
    r = merge(ems_rhino,ids[source_system == 'EMS', .(id1 = main_id, source_system, source_id)], 
              #all.x = T, 
              allow.cartesian = T, 
              by.x = c('source_system1', 'source_id1'),
              by.y = c('source_system', 'source_id'))
    
    r = merge(r,ids[source_system == 'RHINO', .(id2 = main_id, source_system, source_id)], 
              #all.x = T,
              allow.cartesian = T, 
              by.x = c('source_system2', 'source_id2'),
              by.y = c('source_system', 'source_id'))
    
    r[, c('id1', 'id2') := list(ifelse(id1<id2, id1, id2), ifelse(id1>=id2, id1,id2))]
    r =unique(r[, .(id1, id2)])
    if(is.character(mods)){
      if(file.exists(mods)){
        mods = readRDS(mods)
      } else {
        stop('goofy things')
      }
    }
    
    addme = broom::tidy(mods$stack$coefs)
    setDT(addme)

    if(nrow(addme)>0){
      smods = addme[estimate != 0 & term != '(Intercept)', gsub('.pred_1_', '', term, fixed = T)]
    }else{
      smods = NULL
    }
    cvars = c('missing_ssn', 'missing_zip', 'missing_ah')
    r[, c('ens', smods, cvars, 'screen') := NA_real_]
    r[, final := 1]
    cols = dbGetQuery(con, glue::glue_sql('select top(0) * from {`up_table`}', .con = con))
    
    r = r[, .SD, .SDcols = names(cols)]
  
    # Update the results table
    ## Load results to temp
    dbWriteTable(con, '##temp_ems_rhino', value = r)
    
    ## delete existing estimates
    qry = glue::glue_sql('delete ut from {`up_table`} as ut
    left join ##temp_ems_rhino as r
    on ut.id1 = r.id1 AND ut.id2 = r.id2
    where r.id1 IS NOT NULL
                   ', .con = con)
    dbExecute(con, qry)
    qry2 = glue::glue_sql('insert into {`up_table`} select * from ##temp_ems_rhino', .con = con)
    qr = dbExecute(con,qry2)
    
    return(list(qr, rlang::hash(r)))
  
}

#' linkages that are a-priori decided between EMS AND RHINO
#' @param duck path to duckdb
#' @param mods stacking model to predict from
#' @param up_table DBI::Id for the table to update/add rows too
#' @param ... for targets orchestration. Not used
meo_death_fixed = function(duck, mods, up_table, data_version, ...){
  
  con = hhsaw()
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
  on.exit({
    DBI::dbDisconnect(ddb, shutdown = TRUE)
    DBI::dbDisconnect(con)
  })
  dtab = DBI::Id(table = paste0('data_', data_version))
                 
  meo_death = setDT(dbGetQuery(con,"
      WITH DEATH AS (SELECT 
                           DISTINCT
                           events.SOURCE_ID
                           ,PPL.firstname
                           ,PPL.lastname
                           ,EVENTS.event_date
                           ,PPL.source_system
 
                           FROM noharms.outcomes AS EVENTS
                           LEFT JOIN NOHARMS.identifiers AS PPL
                           ON EVENTS.source_id collate Latin1_General_CS_AS = PPL.source_id
                           where events.source_id is not null
                           and PPL.source_system = 'DEATH'),
 
    -- MEO COLUMNS
    MEO AS (SELECT 
                           DISTINCT
                           events.SOURCE_ID
                           ,PPL.firstname
                           ,PPL.lastname
                           ,EVENTS.event_date
                           ,PPL.source_system
 
                           FROM noharms.outcomes AS EVENTS
                           LEFT JOIN NOHARMS.identifiers AS PPL
                           ON EVENTS.source_id collate Latin1_General_CS_AS = PPL.source_id
                           where events.source_id is not null
                           and PPL.source_system = 'MEO')
 
 
    SELECT DEATH.SOURCE_ID as source_id1, DEATH.source_system as source_system1, 
    MEO.source_id as source_id2, MEO.source_system as source_system2
 
    FROM DEATH
    INNER JOIN MEO
    ON DEATH.firstname = MEO.firstname
    AND DEATH.lastname = MEO.lastname
    AND DEATH.event_date= MEO.event_date

                               "))
  
  ids = setDT(dbGetQuery(ddb, 
                         glue::glue_sql(
                           "select main_id, source_system, source_id from {`dtab`} where source_system in ('DEATH', 'MEO') and dob IS NOT NULL", .con = ddb)))
  
  
  r = merge(meo_death,ids[source_system == 'DEATH', .(id1 = main_id, source_system, source_id)], 
            #all.x = T, 
            allow.cartesian = T, 
            by.x = c('source_system1', 'source_id1'),
            by.y = c('source_system', 'source_id'))
  
  r = merge(r,ids[source_system == 'MEO', .(id2 = main_id, source_system, source_id)], 
            #all.x = T,
            allow.cartesian = T, 
            by.x = c('source_system2', 'source_id2'),
            by.y = c('source_system', 'source_id'))
  
  r[, c('id1', 'id2') := list(ifelse(id1<id2, id1, id2), ifelse(id1>=id2, id1,id2))]
  r = unique(r[, .(id1, id2)])
  
  if(is.character(mods)){
    if(file.exists(mods)){
      mods = readRDS(mods)
    } else {
      stop('goofy things')
    }
  }
  
  addme = broom::tidy(mods$stack$coefs)
  setDT(addme)
  if(nrow(addme)>0){
    smods = addme[estimate != 0 & term != '(Intercept)', gsub('.pred_1_', '', term, fixed = T)]
  }else{
    smods = NULL
  }
  cvars = c('missing_ssn', 'missing_zip', 'missing_ah')
  r[, c('ens', smods, cvars, 'screen') := NA_real_]
  r[, final := 1]
  cols = dbGetQuery(con, glue::glue_sql('select top(0) * from {`up_table`}', .con = con))
  
  r = r[, .SD, .SDcols = names(cols)]
  
  # Update the results table
  ## Load results to temp
  dbWriteTable(con, '##temp_meo_death', value = r)
  
  ## delete existing estimates
  qry = glue::glue_sql('delete ut from {`up_table`} as ut
    left join ##temp_meo_death as r
    on ut.id1 = r.id1 AND ut.id2 = r.id2
    where r.id1 IS NOT NULL
                   ', .con = con)
  dbExecute(con, qry)
  qry2 = glue::glue_sql('insert into {`up_table`} select * from ##temp_meo_death', .con = con)
  qr = dbExecute(con,qry2)
  
  return(list(qr, rlang::hash(r)))
  
}


