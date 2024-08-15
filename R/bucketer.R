#' A function to make lsh buckets for the names of folks. Saves to a table called buckets with is long on name (noblank) and has an array column of the bucket IDs
#' @param ducky duckdb connection information
#' @param strtend dataframe with the start and end rows for processing
#' @param b number of bands
#' @param k tokenizer amount
#' @param seed the seed to use for the minhash generator
#' @param minhash minhash generator
#' @param ... targets orchestration
#' 
bucketer = function(duck, strtend, b = 40, k = 3, minhash, ofol, ...){
  
  # duckdb
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1], read_only = TRUE)
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  strtend = strtend[[1]]
  qry = glue::glue_sql('Select * from nms where nm_id >= {strtend$start} AND nm_id < {strtend$end} ', .con = ddb)
# 
  # saveRDS(list(strtend, b, k, minhash, ofol, qry), file.path(ofol, paste0('bucket_', strtend$bid,'.rds')))
  # 
  # return(file.path(ofol, paste0('bucket_', strtend$bid,'.rds')))
#   
  dat = setDT(dbGetQuery(ddb, qry))
  
  # nms = unique(c(dat$first_name_noblank, dat$last_name_noblank))
  nms = dat$name
  names(nms) <- dat$nm_id
  
  tokens = tokenizers::tokenize_character_shingles(nms, n = k)
  mhashes = lapply(tokens, minhash)
  bucket = dt_lsh(mhashes, b)
  bucket[, doc := as.numeric(names(nms)[doc])]

  oot = file.path(ofol, paste0('bucket_', strtend$bid,'.parquet'))
  arrow::write_parquet(bucket, oot)
  
  oot
  
}

#' Prepare the names table and create the minhash generator
#' @param ducky duckdb connection information
#' @param h number of minhash signatures
#' @param seed for minhash
prep_for_lsh = function(duck, h = 200, seed = NULL, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1])
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  dbExecute(ddb, 'drop table if exists nms')
  dbExecute(ddb, 'create table nms (name varchar)')
  dbExecute(ddb,'insert into nms
            select distinct name from (
            select first_name_noblank as name from data
            union
            select last_name_noblank as name from data
            union
            select middle_name_noblank as name from data
            ) as a')
  
  dbExecute(
    ddb,
    glue::glue_sql(
      "
       DROP SEQUENCE if exists nm_seq CASCADE;
       CREATE SEQUENCE if not exists nm_seq START 1;
       alter table nms add column nm_id int default nextval('nm_seq');
       ",
      .con = ddb
    )
  )
  
  # Make the minhash generator
  minhash = textreuse::minhash_generator(h, seed)
  
  return(list(minhash, dbGetQuery(ddb, 'select max(nm_id) as mid from nms')$mid))
  
}

# The bucketer
dt_lsh = function(mhash, bands = 90, offset = 0){
  h <- length(mhash[[1]]) # number of hashes
  d <- length(mhash) # number of documents
  r <- h / bands # number of rows
  
  buckets2 = data.table(
    doc = sort(rep(seq_len(d), h)) + offset,
    hash = unlist(mhash), 
    band = rep(vapply(1:bands, function(i) rep(i, r), integer(r)), d))
  
  buckets2 = buckets2[, .(buckets = digest::digest(list(hash, unique(band)))), .(doc, band)]
  
  buckets2
}

combine_buckets = function(duck, buckets, ...){
  ddb = DBI::dbConnect(duckdb::duckdb(), dbdir = duck[1])
  on.exit(DBI::dbDisconnect(ddb, shutdown = TRUE))
  
  target = DBI::Id(table = 'buckets')
  # dbExecute(ddb, glue::glue_sql('create or replace {`target`} (
  #                               doc integer, buckets list
  #                               )
  #                               ', .con = ddb))
  # Add the rows
  p_files = glue::glue_sql_collapse(paste0("'", buckets,"'"), sep = ', ')
  pq = glue::glue_sql('create or replace table {`target`} as
                      select doc, buckets as bucket from read_parquet([{p_files}])', .con = ddb)
  DBI::dbExecute(ddb, pq)
  
  # convert buckets to numerics
  q2 = glue::glue_sql('create or replace table bucket_ids as
                      select bucket, count(DISTINCT doc) as N from {`target`} group by bucket', .con = ddb)
  dbExecute(ddb, q2)
  
  dbExecute(ddb, 'delete from bucket_ids where N = 1')
  
  dbExecute(
    ddb,
    glue::glue_sql(
      "
       DROP SEQUENCE if exists bucket_seq CASCADE;
       CREATE SEQUENCE if not exists bucket_seq START 1;
       alter table bucket_ids add column bucket_id int default nextval('bucket_seq');
       ",
      .con = ddb
    )
  )

  q3 = glue::glue_sql('create or replace table {`target`} as
                      select l.doc, list(r.bucket_id) as bucket from {`target`} as l
                      inner join bucket_ids as r on l.bucket= r.bucket
                      group by l.doc', .con = ddb)
  dbExecute(ddb, q3)
  
  file_hash = rlang::hash(lapply(buckets, rlang::hash_file))
  
  return(list(target, file_hash))
  
  
}
