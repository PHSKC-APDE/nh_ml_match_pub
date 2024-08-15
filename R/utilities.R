
clean_name_column = function(x){
  
  # Clean up accents
  y = stringi::stri_trans_general(x, id = 'Latin-ASCII')
  y = trimws(toupper(y))
  # clean up punctuation and digits
  # Remove punctuation and digits
  y = gsub(
    "(?!-)[[:punct:]]|[[:digit:]]",
    "",
    y,
    perl = TRUE
  )
  
  # Clean up spaces
  y = gsub("  ", " ", y)
  y[y %in% c('1')] <- 'TRUE'
  y = gsub("[[:punct:]]|[[:space:]]", " ", y)
  
  # clean up JRs and SRs
  y = gsub(' JR$', '', y)
  y = gsub(' III$', '', y)
  y = gsub(' II$', '', y)
  y = gsub(' IV$', '', y)
  y = gsub(' SR$', '', y)
  
  # More spaces
  y = gsub('\\s{2,}',' ',y)
  
  # More triming
  y = trimws(y)
  
  # Remove bad names
  bnames = c("CONSENT", "REFUSED", "FNU", "BABYBOY", "BABYGIRL", "NULL", "DON'T KNOW",
             'REFUSEDCONSENT','DEIDENTIFY', 'CLIENTREFUSED', 'ANONYMOUS', '',
             'THIS', 'REMOVE', "ONE-TWOQ", "UNKNOWNL", 'FOUR-THREEG', 'UNKNOWN',
             'UNKNOWN','SUSPECT', 'CLIENTREFUSED', 'REFUSEDCONSENT',
             'REFUSSED', 'COMMINGLEDREMAINS', 'NOTHUMANREMAINS',
             'NOLASTNAME','ANONYMOUS','REFUSSED', 'NON FORENSIC','FAKE',
             'FAKE TH AND KING', "FAKE LAST NAME", ''
  )
  y[y %in% bnames] <- NA
  
  y
  
}

remove_spaces = function(x) gsub(' ', '', x, fixed = T)

nb = function(x) {
  x = as.character(x)
  x[is.na(x)] <- ''
  x
}

make_hash = function(x){
  x = lapply(x, nb)
  x$sep = '|'
  openssl::md5(do.call(paste, x))
}

hhsaw = function(){
  con = DBI::dbConnect(odbc::odbc(),
                       driver ='ODBC Driver 18 for SQL Server',
                       server = '[SERVER REDACTED]',
                       database = '[DATABASE REDACTED]',
                       uid = keyring::key_list('hhsaw')[["username"]],
                       pwd = keyring::key_get('hhsaw',
                                              keyring::key_list('hhsaw')[["username"]]),
                       Encrypt = 'yes',
                       TrustServerCertificate = 'yes',
                       Authentication = 'ActiveDirectoryPassword',
                       encoding = 'latin1')
}

loadspatial = function(ddb){
  DBI::dbExecute(ddb, 'install spatial; load spatial;')
}
