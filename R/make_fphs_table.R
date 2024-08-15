make_fphs_table = function(...){
  con = hhsaw()
  dots = list(...)
  dbExecute(con, "truncate table fphs_services.noharms_id_linkage;")
  dbExecute(con, "
             
    insert into  fphs_services.noharms_id_linkage 
        Select *
        from noharms.id_linkage
        where source_system in (
               'DEATH',
               'EMS',
               'KC_JAIL',
               'MUNI_JAIL',
               'MEDICAID'
        );

             ")
  
}