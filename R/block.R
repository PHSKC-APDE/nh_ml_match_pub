#' create blocking tables
#' @param duck connection to duckdb
#' @param blinks links to the block parquet files
#' @param ... for orchestration purposes (prevents read/write conflicts)
block = function(duck, blinks, data_version, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1])
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  target = DBI::Id(table = paste0('blocks_', data_version))
  # Initialize the block table
  DBI::dbExecute(ddb, 
                 glue::glue_sql('create or replace table {`target`} (id1 bigint, id2 bigint, ss1 varchar, ss2 varchar)', .con = ddb))
  

  
  # Add the rows
  p_files = glue_sql_collapse(paste0("'", blinks,"'"), sep = ', ')
  pq = glue::glue_sql('insert into {`target`} 
                      select distinct id1, id2, ss1, ss2 from read_parquet([{p_files}])
                      ', .con = ddb)
  DBI::dbExecute(ddb, pq)

  # overall row ID for blocks
  dbExecute(
    ddb,
    glue::glue_sql(
      "
       DROP SEQUENCE if exists bid_seq CASCADE;
       CREATE SEQUENCE if not exists bid_seq START 1;
       alter table {`target`} add column bid bigint default nextval('bid_seq');
       ",
      .con = ddb
    )
  )

  # 
  # # Make tables by whether source system agrees
  # # And then give them IDs
  # dbExecute(
  #   ddb,
  #   glue::glue_sql(
  #     '
  #     drop table if exists within;
  #     create table within as
  #     Select * from {`target`}
  #     where ss1 = ss2
  #     ',
  #     .con = ddb
  #   )
  # )
  #   
  # dbExecute(
  #   ddb,
  #   glue::glue_sql(
  #     "
  #      DROP SEQUENCE if exists within_seq CASCADE;
  #      CREATE SEQUENCE if not exists within_seq START 1;
  #      alter table within add column rid bigint default nextval('within_seq');
  # 
  #      ",
  #     .con = ddb
  #   )
  # )
  # 
  # dbExecute(
  #   ddb,
  #   glue::glue_sql(
  #     '
  #     drop table if exists between;
  #     create table between as
  #     Select * from {`target`}
  #     where ss1 != ss2
  #     ',
  #     .con = ddb
  #   )
  # )
  # dbExecute(
  #   ddb,
  #   glue::glue_sql(
  #     "
  #      DROP SEQUENCE if exists between_seq CASCADE;
  #      CREATE SEQUENCE if not exists between_seq START 1;
  #      alter table between add column rid bigint default nextval('between_seq');
  # 
  #      ",
  #     .con = ddb
  #   )
  # )
  # 
  # 
  # # Drop blocks now that it has been split up
  # DBI::dbExecute(ddb, 'drop table if exists {`target`}')
  
  # Count the number of rows for each chunk
  # Nw = DBI::dbGetQuery(ddb, 'Select count(*) as N from within')$N
  # Nb = DBI::dbGetQuery(ddb, 'Select count(*) as N from between')$N
  # 
  N = DBI::dbGetQuery(ddb, glue::glue_sql('select count(*) as N from {`target`}', .con = ddb))
  DBI::dbDisconnect(ddb, shutdown = T)
  
  return(N)

}

make_block_var_table = function(duck, data_version, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1])
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  dtab = DBI::Id(table = paste0('data_', data_version))
  btab = DBI::Id(table = paste0('blockvars_', data_version))
  # Create the dataset to generate blocks from
  q = glue::glue_sql("
            create or replace table {`btab`} as (
              select
              main_id,
              ssn,
              right(ssn,4) as ssn_last4,
              dob,
              year(dob) as dob_year,
              month(dob) as dob_month,
              day(dob) as dob_day,
              d.first_name_noblank,
              d.last_name_noblank,
              substring(d.first_name_noblank,1,1) as fn1,
              substring(d.first_name_noblank,2,1) as fn2,
              substring(d.last_name_noblank,1,1) as ln1,
              substring(d.last_name_noblank,2,1) as ln2,
              left(d.first_name_noblank,2) as fn2l,
              left(d.last_name_noblank, 2) as ln2l,
              gender as sex,
              source_id,
              source_system,
              CONCAT(source_id,'|',source_system) as ssid
              from {`dtab`} as d

            
          )", .con = ddb)
  DBI::dbExecute(ddb, q)
  
  # overall row ID for blockvars
  dbExecute(
    ddb,
    glue::glue_sql(
      "
         DROP SEQUENCE if exists bd_seq CASCADE;
         CREATE SEQUENCE if not exists bd_seq START 1;
         alter table {`btab`} add column bvid bigint default nextval('bd_seq');
         ",
      .con = ddb
    )
  )
  
  list(q, dbGetQuery(ddb, glue::glue_sql('select count(*) from {`btab`}', .con = ddb))[[1]])
}

make_block = function(duck, q, ofol, data_version, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = T)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  qry = q$V1
  i = q$qid
  whr = q$where
  
  stopifnot(length(i) == 1)
  
  btab = DBI::Id(table = paste0('blockvars_', data_version))
  output = file.path(ofol, paste0('qry_',i,'.parquet'))
  r <- glue_sql("
    copy (
      select
      l.main_id as id1,
      l.source_system as ss1,
      r.main_id as id2,
      r.source_system as ss2,
      {i} as qid
      from {`btab`} as l
      inner join {`btab`} as r on {qry}
      where l.main_id < r.main_id
      AND (r.ssid != l.ssid)
      {whr}
      AND NOT (l.source_system = 'DEATH' AND r.source_system = 'DEATH') -- no between joins for death
      AND 1 = 1 -- salting, maybe it helps?
      order by 1
    ) TO {output} (FORMAT 'parquet');
    
  ", .con = ddb)
  
  e = dbExecute(ddb, r)
  
  return(output)

  
}
