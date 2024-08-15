bcp_load_hhsaw = function(dataset, table, user, pass, con, tabcheck = F){
  
  
  # Make sure the table exists
  if(tabcheck) stopifnot(dbExistsTable(con, table))
  
  if(is.character(dataset) && tools::file_ext(dataset) == 'rds'){
    dataset = readRDS(dataset)
  }
  if(is.character(dataset) && tools::file_ext(dataset) == 'parquet'){
    dataset = arrow::read_parquet(dataset)
  }
  
  if(is.character(dataset)){
    filepath = dataset
    if(!tools::file_ext(dataset) == 'txt') stop('this function requires a data.frame or tab delimited .txt file as input for `dataset`')
  }else{
    filepath = tempfile(fileext = '.txt')
    on.exit({
      if(file.exists(filepath)) file.remove(filepath)
    })

    data.table::fwrite(dataset, filepath, sep = '\t')

  }
  
  schema = unname(table@name['schema'])
  table_name = unname(table@name['table'])
  
  # Set up BCP arguments and run BCP
  bcp_args <- paste(glue::glue('{schema}.{table_name} IN'),
                    glue::glue('"{filepath}"'),
                    '-r \\n',
                    '-C 65001',
                    '-F 2',
                    '-S "HHSAW"',
                    '-d hhs_analytics_workspace',
                    '-b 500000',
                    '-c',
                    '-G',
                    glue::glue('-U {user}'),
                    glue::glue('-P {pass}'),
                    '-D'
                    )
    

  
  # Load
  # sys::exec_wait(cmd = r"(C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe)", args = I(bcp_args), std_out = T, std_err = T)
  a = system2(command = "bcp", args = c(bcp_args), stdout = TRUE, stderr = TRUE)
  
  status = attr(a, 'status')
  if(length(status)>0 && status == 1){
    stop(a)
  }
  
  
  a
}
