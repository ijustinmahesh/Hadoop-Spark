package org.etl.stage
import org.inceptez.hack

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.Column

object partb extends reusable_target{
  
   
  def main(args:Array[String]) {
    
       val csvschema =  StructType(
                    Array(
                    StructField("IssuerId"      ,IntegerType,false)
                   ,StructField("IssuerId2"     ,IntegerType,false)
                   ,StructField("BusinessDate"  ,DateType   ,true)
                   ,StructField("StateCode"     ,StringType ,true)
                   ,StructField("SourceName"    ,StringType ,true)
                   ,StructField("NetworkName"   ,StringType ,false)
                   ,StructField("NetworkURL"    ,StringType ,false)
                   ,StructField("custnum"       ,StringType ,true)
                   ,StructField("MarketCoverage",StringType ,true)
                   ,StructField("DentalOnlyPlan",StringType ,true)
                             )    )
        val spark=SparkSession.builder
                              .appName("hackathon")
                              .master("local[*]")
                              .config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
                              .config("spark.eventLog.dir", "file:////tmp/spark-events")
                              .config("spark.eventLog.enabled", "true")
                              .config("hive.metastore.uris","thrift://localhost:9083")
                              .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
                              .config("spark.sql.shuffle.partitions",10)
                              .enableHiveSupport()
                              .getOrCreate();
                     
        spark.sparkContext.setLogLevel("error")
        val dfcsvins1=spark.read.option("delimiter",",")
                            .option("inferschema","true")
                            .option("header","true")
                            .option("escape","'")
                            .option("mode","dropmalformed")
                            .option("dateFormat","yyyy-mm-dd")
                            .schema(csvschema)                            
                            .csv("hdfs://localhost:54310/user/hduser/sparkhack2/insura*1.csv*")
                            
       val dfcsvins2=spark.read.option("delimiter",",")
                            .option("inferschema","true")
                            .option("header","true")
                            .option("escape","'")
                            .option("mode","dropmalformed")
                            .option("dateFormat","dd-mm-yyyy")
                            .schema(csvschema)                            
                            .csv("hdfs://localhost:54310/user/hduser/sparkhack2/insura*2.csv*")
         val dfcsvins1_renamed_concat = dfcsvins1.withColumnRenamed("StateCode" , "stcd") 
                                                 .withColumnRenamed("SourceName", "srcnm")
                                                 .withColumn("issueridcomposite", concat(col("IssuerId").cast("String")
                                                                                        ,col("IssuerId2").cast("String")
                                                                                        )
                                                             )
                                                 .drop("DentalOnlyPlan")        
                                                 .withColumn("sysdt", current_date())
                                                 .withColumn("systs", date_format(current_timestamp(),"HHMM"))
         println(dfcsvins2.count())       
         dfcsvins1_renamed_concat.show()
         val dfcsvins1_drop_null = dfcsvins1_renamed_concat.na.drop("any").count()
         
         println(dfcsvins1_drop_null)
         
         
         import org.apache.spark.sql.functions.udf
         val allmethod_udf_object= new hack.allmethod       
         val calludf_strreplace = udf(allmethod_udf_object.remspecialchar _)
         dfcsvins1_renamed_concat.printSchema()
         val network_formated    = dfcsvins1_renamed_concat.withColumn("cleansed_Networkname",
                                                                    when (col("NetworkName").isNotNull,
                                                                                 calludf_strreplace(col("NetworkName")
                                                                         )).otherwise("Unknown")
                                                                         ).drop("NetworkName")
         //val network_formated    = dfcsvins1_renamed_concat.na.fill("Unknown",(Array("_NetworkName")))
                                                                                
   
         network_formated.coalesce(1).write.mode("overwrite")
                                           .csv("hdfs://localhost:54310/user/hduser/sparkhack2/outputcsv/")
         
         network_formated.write.mode("append").option("path", "hdfs://localhost:54310/user/hduser/sparkhack2/stage")
                                              .saveAsTable("hackathon.stage_insurance")
                                            
                                                    
                  
                                            
         val custstates = spark.sparkContext.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states*")
                                            .cache()
         val custfilter =custstates.map(x=>x.split(",",-1))
                                           .filter(y=> (y.length ==5) )
         val statesfilter =custstates.map(x=>x.split(",",-1))
                                           .filter(y=> (y.length ==2) )
                                           
         val custstates_df =spark.read
                            .option("delimiter",",")
                            .option("inferschema","true")
                            .option("header","false")
                            .option("escape","'")
                          //  .option("mode","dropmalformed")
                            .option("nullValue","na")
                            .option("nanValue","0")
                            .csv("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states*")
         custstates_df.show(800000)
         val statesfilterdf =custstates_df.where("upper(_c0)!=lower(_c0)")
                                          .select(col("_c0").alias("statecode"),col("_c1").alias("statedesc"))
         val custfilterdf   =custstates_df.where("upper(_c0)=lower(_c0)")
                                           .select(  col("_c0").alias("custcode"),col("_c1").alias("custname")
                                                    ,col("_c2").alias("lname")   ,col("_c3").alias("age")
                                                    ,col("_c4").alias("proffession"))
                                          
         custfilterdf.createOrReplaceTempView("custview")
         statesfilterdf.createOrReplaceTempView("statesview")   
         statesfilterdf.show()
         custfilterdf.show()
         val remspecialcharudf =  udf(allmethod_udf_object.remspecialchar _)
         println(custstates_df.printSchema())
         println(statesfilterdf.printSchema())
         
         val columnList = network_formated.schema.fields.map(field =>((field.name.toLowerCase())))
         println(columnList)
         val network_df = network_formated.toDF(columnList: _* )

 
         network_df.createOrReplaceTempView("network")
         
         /*val dfcsvins1_txnfomred  = spark.sql("""
                                                  SELECT *
                                                          ,case
                                                                 when instr(n.networkurl, 'http')  > 0 then 'httpnonsecured'
                                                                 when instr(n.networkurl,'https') > 0 then 'httpsecured'
                                                           else  'noprotocol' 
                                                           end as protocol
                                                          ,current_date()        as sysdt
                                                          ,current_timestamp()   as systm
                                                          ,year(BusinessDate)    as  year    
                                                          
                                                  FROM    network n
                                              """
         
         )*/
         network_df.printSchema()
         
         val dfcsvins1_txnfomred  = spark.sql("""SELECT  n.* 
                                                         ,case
                                                              when instr(n.networkurl,'https')  > 0 then 'httpsecured'
                                                              when instr(n.networkurl,'http')   > 0 then 'httpnonsecured'
                                                           else  'noprotocol'
                                                           end as protocol
                                                           ,n.businessdate,year(n.businessdate) year,month(n.businessdate) m
                                                           ,s.statedesc as statesdesc
                                                           ,c.custname,c.age  as age,c.proffession as proffession  
                                                 FROM  network n inner join statesview s
                                                             on (n.stcd=s.statecode)
                                                       inner join custview c
                                                             on (n.custnum=c.custcode)
                                              """)
                                              // 
         dfcsvins1_txnfomred.show()
        // custstates_len.show(10)
         dfcsvins1_txnfomred.createOrReplaceTempView("dfcsvins1_txnfomred_vw")
         val agg_insurance_sql   = spark.sql(""" 
                                      SELECT
                                               row_number() over(partition by  protocol
                                                                 order by 
                                                                          count(protocol) desc
                                                                 
                                                                ) as seqno   
                                               ,avg(age) as avg_age                                                 
                                               ,count(protocol) as count
                                               ,statesdesc,protocol,proffession
                                      FROM     dfcsvins1_txnfomred_vw
                                      group by 
                                                statesdesc
                                                ,protocol
                                                ,proffession
                                     order by protocol,1           
                                  """)
         //agg_insurance_sql.show(100)
         
         val jdbc_conn = new reusable_target
         val jdbc_url  =jdbc_conn.db_writer("xxx")
         val dbcred=new java.util.Properties();
         dbcred.put("user", "root")
         dbcred.put("password", "Root123$")
         dbcred.put("driver", jdbc_url._2)
         agg_insurance_sql.write.mode("append").jdbc(jdbc_url._1,"insureaggregated",dbcred)
         
         println("completed")                  
         
  }
  
}