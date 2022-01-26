package org.etl.stage

import org.apache.spark._
import java.io.FileInputStream
import java.util.Properties

import org.etl.stage.RuntimeOptions
import org.apache.spark.storage.StorageLevel



object parta extends RuntimeOptions {
  
  case class insureclass(IssuerId:String,
                        IssuerId2:String,
                        BusinessDate:String,
                        StateCode:String,
                        SourceName:String,
                        NetworkName:String,
                        NetworkURL:String,
                        custnum:String,
                        MarketCoverage:String,
                        DentalOnlyPlan:String);
  def cleanse_file(sparkcontext:org.apache.spark.SparkContext ,fileurl:String): org.apache.spark.rdd.RDD[insureclass]= {
    
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
    val clean_schemaed_insurance_data:org.apache.spark.rdd.RDD[insureclass] = insurance_clean.map(x=>insureclass(
                                                                                                  x(0),x(1),x(2),x(3),
                                                                                                  x(4),x(5),x(6),x(7),
                                                                                                  x(8),x(9)
                                                                                                   )
                                                                                                 )
    return clean_schemaed_insurance_data
  }
   
  def main(args:Array[String]) {
    
    
    
    val appName     =conn.getProperty("appname")    
    val hdfsurl     =conn.getProperty("hdfsurl")
    val repartition =conn.getProperty("partitions").toInt
    val setMaster   =s"local[${conn.getProperty("setmaster")}]"
    val sparkconf   =new SparkConf().setAppName(appName).setMaster(setMaster)
    val sparkcontext=new SparkContext(sparkconf)
    sparkcontext.setLogLevel("ERROR")
    
    
    val cleansed_data_1:org.apache.spark.rdd.RDD[insureclass]=cleanse_file(sparkcontext,hdfsurl+"user/hduser/sparkhack2/insuranceinfo1*") 
    val cleansed_data_2:org.apache.spark.rdd.RDD[insureclass]=cleanse_file(sparkcontext,hdfsurl+"user/hduser/sparkhack2/insuranceinfo2*")
    
    val insuredatamerged =cleansed_data_1.union(cleansed_data_2).persist(StorageLevel.MEMORY_ONLY_SER)
    val insuredatamerged_distinct =insuredatamerged.distinct()
    val insuredatamerged_repartition =insuredatamerged_distinct.repartition(8) 
    println(insuredatamerged.count())
    println(insuredatamerged.count() -insuredatamerged.distinct().count())
    val rdd_20191001 = insuredatamerged_repartition.filter(x=>x.BusinessDate=="2019-10-01")
    val rdd_20191002 = insuredatamerged_repartition.filter(x=>x.BusinessDate=="2019-10-02")
    val spark = org.apache.spark.sql.SparkSession.builder().appName("WD26 Hack").master("local[2]").getOrCreate()
    import spark.implicits._ 
    rdd_20191001.saveAsTextFile(hdfsurl+"user/hduser/sparkhack2/output/")
    val insuredaterepartdf =insuredatamerged_repartition.toDF()
     }
}