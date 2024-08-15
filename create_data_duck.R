# initialize a new duck
# And craete a crosswalk between the old way of doing hashes and the new way
library('targets')
library('duckdb')
library('data.table')
library('stringr')
tar_source()

datadb = DBI::dbConnect(duckdb(), '[FILEPATH REDACTED]ducks/data.duckdb')
DBI::dbExecute(datadb, 'install spatial; load spatial;')
dbExecute(datadb, 'CREATE SEQUENCE seq_cid START 1')
dbExecute(datadb, "create table hash_hist (
          cid integer primary key default nextval('seq_cid'),
          source_system varchar(255) NOT NULL,
          source_id varchar(255) not null,
          clean_hash varchar(32) not null
)")

dta = clean_noharms_ids(T)
dta = unique(dta)

dbWriteTable(datadb, 'hash_hist', 
             value = unique(dta[, .(source_system, source_id, clean_hash)]), append = T)

res = dbGetQuery(datadb, 'select * from hash_hist limit 10')
hh = dbGetQuery(datadb, 'select cid, clean_hash from hash_hist')
dta = merge(dta, hh, by = 'clean_hash')

saveRDS(unique(dta[, .(main_id, cid)]), '[FILEPATH REDACTED]ducks/id_hash_method_xw.rds')
