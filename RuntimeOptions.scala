package org.etl.stage
import java.util.Properties
import java.io.FileInputStream


trait RuntimeOptions {
  
  val runtimeoptions_file = "/hackathon_ijm/connection.prop"
  val conn     =new Properties()
  val propFile = new FileInputStream(runtimeoptions_file) //
  conn.load(propFile)
  
}