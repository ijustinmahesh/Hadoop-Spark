package org.etl.stage

class reusable_target extends RuntimeOptions  {
  
  def db_writer(table_name:String):(String,String) = {
    
    
    val dbtype     =conn.getProperty("db_type")
    val dbname     =conn.getProperty("db_name")    
    val dbhost     =conn.getProperty("db_host")
    val dbport     =conn.getProperty("db_port")
    val dbusername =conn.getProperty("db_username")
    val dbpwd      =conn.getProperty("db_pwd")
    val driver     =conn.getProperty("driver")
    val url="jdbc:"+dbtype+"://"+dbhost+"/"+dbname
    return (url,driver)
    
  }
  
  
}

#cu935
user build
Collabnet1!