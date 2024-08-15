upload_table = function(val, target, overwrite = T, append = F, usebcp = T){
  if(is.character(val)) val = readRDS(val)
  
  con = hhsaw()
  on.exit(DBI::dbDisconnect(con))
  
  if(!usebcp){
    dbWriteTable(con, target, value = val, overwrite = overwrite, append = append)
  }else{
    dbWriteTable(con, target, value = val[0,], overwrite = overwrite, append = append)
    bcp_load_hhsaw(val, target, 'dcasey@kingcounty.gov', keyring::key_get('hhsaw', 'dcasey@kingcounty.gov'),con = con,tabcheck = T)
  }

  rlang::hash(val)
}