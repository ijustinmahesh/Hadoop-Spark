package org.etl.stage
import org.apache.spark.sql.SparkSession

class cleanse {
  

  
  def cleanse_file(sparkcontext:org.apache.spark.SparkContext ,fileurl:String) {
    
    val insurance_src=sparkcontext.textFile(fileurl)
    val rawinsurance_counter=insurance_src.count()
    println("before filter"+rawinsurance_counter)
    //val srcinsurancehdr=srcinsuranceraw.first()
    //val insurancefiltrd=srcinsuranceraw.filter(x=> x !=srcinsurancehdr) 
    //println(insurancefiltrd.filter(x=>x.contains("footer")).count())
    //val hdrfiltrddcnt   =insurancefiltrd.count()
    val insurance_hdrftrblank_filter =insurance_src.zipWithIndex().collect 
                                {
                                case(data,index)  
                                if (index > 0 && index < rawinsurance_counter-1 && data.size > 0 ) => data.trim()
                                 }
    //val insurance_filter_blank = insurance_hdrftrblank_filter.filter(x=> x.size > 0 )
    println(insurance_hdrftrblank_filter.count)
    val cntfinal = insurance_hdrftrblank_filter.count
    val firstlastrow = insurance_hdrftrblank_filter.zipWithIndex()
                                                   .collect { case(data,index) 
                                                              if index ==cntfinal-1 || index==0  => data 
                                                            }
    
    
    
    val insurance_split = insurance_hdrftrblank_filter.map(x=> x.split(",",-1)).cache()
    val insurance_clean =insurance_split.filter(x=>x.length ==10)                                                        
    val reject_data     =insurance_split.filter(x=>x.length !=10).map(x=>(x.length,x(0)))
    println(insurance_hdrftrblank_filter.first())
    println(insurance_clean.count())
    val x =reject_data.collect()
    x.foreach(println)
    //println(x.deep.mkString("\n"))
    
    
    println("completed")
 
  }
   
 
  
}