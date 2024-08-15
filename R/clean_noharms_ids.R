clean_noharms_ids = function(hash_only = FALSE){
  con = hhsaw()
  on.exit(dbDisconnect(con))
  isel = DBI::SQL('i.hash_id')
  
  if(hash_only) isel = glue::glue_sql('{isel} AS main_id,', .con = con)
  if(!hash_only) isel = SQL('')

  # check to make sure there are no new gender doohickeys
  thegenders = c("X", "O", "F", "Unknown (Unable to Determine)", "Not Recorded", 
                 "Other, neither exclusively male or female", "Male", "Male-to-Female, Transgender Female", 
                 " F", "M", "Female-to-Male, Transgender Male", "UNMAPPED", "Non-Binary", 
                 "U", "Not Applicable", "Female", NA, "C", "Unknown")
  gndr = setDT(dbGetQuery(con, 'select distinct gender from noharms.identifiers'))
  stopifnot(all(gndr$gender %in% thegenders))
  
  # Categorize gender into Consistently Male, Consistently Female, Consitently Other, and Unknown
  gxw = data.table(
    gender = thegenders,
    gender_new = c("X", "X", "F", "U", "U", 
                   "X", "M", "X", 
                   "F", "M", "X", "U", "X", 
                   "U", "U", "F", NA, "U", "U")
  )
  
  
  # Start with identifeirs and demographics
  # handle addresses later
  subq = glue::glue_sql("
    SELECT distinct 
    {isel}
    i.source_id, source_system, trim(upper(firstname)) as firstname, 
    trim(upper(middlename)) as middlename, 
    trim(upper(lastname)) as lastname, trim(ssn) as ssn, dob, trim((gender)) as gender
    FROM noharms.identifiers as i
    ", .con = con)
  ids <- setDT(dbGetQuery(con, subq))
  ids = merge(ids, gxw, by = 'gender', all.x = T)
  ids[, gender := NULL]
  setnames(ids, 'gender_new', 'gender')
  # , zip, geo_hash_geocode
  
  # Clean names ----
  ## Some dataset specific changes ----
  ids[source_system %in% c('KC_JAIL', 'BHDATA') & str_detect(firstname, " ") & is.na(middlename), 
        middlename := str_sub(firstname, str_locate(firstname, " ")[, 1] + 1, -1L)]
  ids[source_system %in% c('KC_JAIL', 'BHDATA') & str_detect(firstname, " "), 
        firstname := str_sub(firstname, 1L, str_locate(firstname, " ")[, 1] - 1)]
  
  ## General changes to names ----
  ids[, c('first_name_clean', 'middle_name_clean', 'last_name_clean') := lapply(.SD, clean_name_column), .SDcols = c('firstname', 'middlename', 'lastname')]
  ids[first_name_clean %in% c("JANE", "ONE-TWOQ", "UNKNOWNL", 'JOHN', 'JON', 'DOE', 'ONE', 'J') & last_name_clean == "DOE", `:=` (first_name_clean = NA_character_, last_name_clean = NA_character_)]
  
  # Clean SSNs ----
  ids[, ssn_num := as.numeric(ssn)]
  ids[, ssn_clean := ssn]
  ids[ssn_num >= 900000000 | ssn_num == 0 | ssn == 555555555 | nchar(ssn)<4, ssn_clean := NA] # nchar(ssn) != 9
  rssn = sapply(0:8, function(s) paste(rep(s, 9), collapse = ''))
  ids[ssn_num %in% rssn, ssn_clean := NA]
  ids[!is.na(ssn_clean) & nchar(ssn_clean)>=7, ssn_clean := stringr::str_pad(ssn_clean, 9, 'left', '0')]
  ids[, ssn_num := NULL]
  
  # Clean date of birth ----
  ids[, dob_clean := as.Date(dob, tryFormats = c('%Y-%m-%d', '%m/%d/%Y'))]
  ids[year(dob_clean) > 3000 | year(dob_clean) < 1901, `:=` (dob_clean = NA)]
  
  # Fix sex ----
  ids[, gender_clean := gender]
  ids[gender_clean %in% c('U'), gender_clean := NA]
  
  
  # some hyrule routines ----
  # NOTE: the hyrule routine doesn't work because there are multiple rows per hash id at this stage
  # ldat = hyrule::prep_data_for_linkage(ids, 'firstname', 'lastname', 'dob', 'middlename', zip = NULL, ssn = 'ssn', sex = 'gender', id = 'main_id')
  ## sex ----
  ids[, gender_clean := toupper(substr(gender_clean,1,1))]
  stopifnot('gender_clean column must be convertible to "M", "F", and/or "X"' = all(ids[!is.na(gender_clean), gender_clean %in% c('M', 'F', 'X')]))
  
  ## create "noblank" columns ----
  ids[, c('first_name_noblank', 'middle_name_noblank', 'last_name_noblank') :=
        list(remove_spaces(first_name_clean), remove_spaces(middle_name_clean), remove_spaces(last_name_clean))]
  
  bnames = c("CONSENT", "REFUSED", "FNU", "BABYBOY", "BABYGIRL", "NULL", "DON'T KNOW",
             'REFUSEDCONSENT','DEIDENTIFY', 'CLIENTREFUSED', 'ANONYMOUS', '',
             'THIS', 'REMOVE', "ONE-TWOQ", "UNKNOWNL", 'FOUR-THREEG', 'UNKNOWN',
             'UNKNOWN','SUSPECT', 'CLIENTREFUSED', 'REFUSEDCONSENT',
             'REFUSSED', 'COMMINGLEDREMAINS', 'NOTHUMANREMAINS',
             'NOLASTNAME','ANONYMOUS','REFUSSED', 'NON FORENSIC','FAKE',
             'FAKE TH AND KING', "FAKE LAST NAME", '', 'NONFORENSIC',
             'FAKETHANDKING', "FAKELASTNAME","DON'TKNOW",'FOURTHREEG',
             'MALEAFRICANAMERICAN', 'FIRSTNAME', 'LASTNAME', 'UNK', 'NONE',
             'BV', 'NONAMEGIVEN', 'NONAME', 'DOETRAUMA', 'NOTAPPLICABLE',
             'UNABLETOCOMPLETE', 'NOTRECORDED', 'DELETE', 'TRAINING'
  )
  
  ids[first_name_noblank %in% bnames, first_name_noblank := NA]
  ids[last_name_noblank %in% bnames, last_name_noblank := NA]
  ids[middle_name_noblank %in% bnames, middle_name_noblank := NA]
  
  
  # consolidate information within source_ids/source_systems ----
  nms = c('first_name_noblank', 'middle_name_noblank', 'last_name_noblank', 'ssn_clean', 'dob_clean', 'gender_clean')
  
  setorder(ids, source_system, source_id, first_name_noblank, last_name_noblank)
  
  fillblanks = function(x){
    Nu = length(unique(x))
    aNA = any(is.na(x))
    if(aNA == TRUE & Nu == 2){
      x[] <- unique(na.omit(x))
    }
    
    x
  }
  
  ids[, (nms) := lapply(.SD, fillblanks), .SDcols = nms, by = c('source_system', 'source_id')]
  
  # A second pass for the rare instances of probably multiple people within a system-id pair
  # Turned off for now because it takes too long
  # ids[, setdiff(nms, c('first_name_noblank', 'last_name_noblank')) := lapply(.SD, fillblanks), .SDcols = nms, by = c('source_system', 'source_id', 'first_name_noblank', 'last_name_noblank')]
  # 
  
  ids[, clean_hash := make_hash(.SD), 
      .SDcols = c('source_system', 'source_id', 'first_name_noblank', 'middle_name_noblank',
                  'last_name_noblank', 'ssn_clean', 'dob_clean', 'gender_clean')]
  
  # Rows must have first name, last name, and dob OR an ssn
  ids = ids[!(is.na(first_name_noblank) & is.na(last_name_noblank) & !is.na(dob)) | !is.na(ssn)]
  
  if(hash_only){
    return(unique(ids[, .(main_id, source_system, source_id, clean_hash)]))
  }
  
  # Don't keep the intermediate stuff
  # the code below will have to be changed if this approach changes
  ids = unique(ids[, .(source_system, 
                       source_id, 
                       first_name_noblank, 
                       middle_name_noblank,
                       last_name_noblank, 
                       ssn  = ssn_clean, 
                       dob = dob_clean, 
                       gender = gender_clean,
                       clean_hash)])
  
  # Add some additional variables
  ## dob parts ----
  stopifnot('`dob` variable must be of type Date' = hyrule::is.Date(ids[['dob']]))
  ids[, paste0('dob_clean_', c('year', 'month', 'day')) := list(year(dob), month(dob), mday(dob))]
  
  ## middle initial ----
  ids[, middle_initial := substr(middle_name_noblank,1,1)]
  
  
  return(ids)

  
}

