package org.etl.stage
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.sql.types._

import org.elasticsearch.spark.sql._

// Kafka packages
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object partc {
	def main(args:Array[String])
	{
		val spark=SparkSession.builder().enableHiveSupport().appName("Usecase1 kafka to sql/nosql")
				.master("local[*]")
				.config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
				.config("spark.eventLog.dir", "file:////tmp/spark-events")
				.config("spark.eventLog.enabled", "true") // 3 configs are for storing the events or the logs in some location, so the history can be visible
//				.config("hive.metastore.uris","thrift://localhost:9083") //hive --service metastore
//				.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse") //hive table location
				.config("spark.sql.shuffle.partitions",10)
				.config("spark.es.nodes","localhost") 
				.config("spark.es.port","9200")
				.config("es.nodes.wan.only",true)
				.enableHiveSupport().getOrCreate();
		 val ssc=new StreamingContext(spark.sparkContext,Seconds(1))
     spark.sparkContext.setLogLevel("error")
     
		 val topics = Array("custdatajson")
		 val kafkaParams = Map[String, Object](
				"bootstrap.servers"   -> "localhost:9092,localhost:9093,localhost:9094",
				"key.deserializer"    -> classOf[StringDeserializer],
				"value.deserializer"  -> classOf[StringDeserializer],
				"group.id"            -> "hackathon",
				"auto.offset.reset"   -> "earliest"
				,"enable.auto.commit" -> (false:java.lang.Boolean) )
		val dstream = KafkaUtils.createDirectStream[String, String](ssc,
                                                    						PreferConsistent, 
						                                                    Subscribe[String, String](topics, kafkaParams)
						                                                    )
			
     try
		 {
         import spark.sqlContext.implicits._
				           dstream.foreachRDD{
			                 rddfromdstream =>
			                     val offsetranges=rddfromdstream.asInstanceOf[HasOffsetRanges].offsetRanges
			                                               //println("Printing Offsets")
                           //offsetranges.foreach(println)
			                     if(!rddfromdstream.isEmpty())
			                        {
			                          val jsonrdd =  rddfromdstream.map(x=> x.value())
			                          //jsonrdd.foreach(println)
			                          val jsondf =spark.read.option("multiline", "true").option("mode", "DROPMALFORMED").json(jsonrdd)
			                          jsondf.createOrReplaceTempView("customerapidata")
			                          
			                          val dfchec = spark.sql(
			                                       """Select custid,cell,first
			                                                 ,age,email,city
			                                                 ,concat(latitude,',',longitude) as coordinates 
			                                                 ,dob,dob_date
			                                                 ,country,state,timezone
			                                                 ,curdt,curts
			                                         From       
			                                                 (select  explode(results) as res,
			                                                          res.login.username    as custid
			                                                          ,res.cell       as cell 
			                                                          ,res.name.first as first			                                                                                    
			                                                          ,res.dob.age    as age
			                                                          ,res.email      as email
			                                                          ,res.location.city as city
			                                                          ,res.location.coordinates.latitude  as latitude
			                                                          ,res.location.coordinates.longitude as longitude
			                                                          ,to_date(res.dob.date,'yyy-mm-dd') dob
			                                                          ,cast(round((datediff(current_date,to_date(res.dob.date,'yyy-mm-dd'))/365)) as integer) agecompute  
			                                                          ,dayofmonth(res.dob.date)    as dob_date	                                                                                    
			                                                          ,case when  cast(round((datediff(current_date,to_date(res.dob.date,'yyy-mm-dd'))/365)) as integer) = res.dob.age
			                                                          then  	'valid'
			                                                          else 'invalid'
			                                                          end ageflag		                                                                                                                  
			                                                          ,res.location.country  as country
			                                                          ,res.location.state    as state
			                                                          ,res.location.timezone as timezone
			                                                          ,current_date              as curdt
			                                                          ,current_timestamp         as curts
                                                       from   customerapidata
                                                       ) as innerq 
                                                 where age  >35
                                                 """)
                                                                        dfchec.show(10,false)
                                                                        println("es")
                                                        dfchec.saveToEs("sparkjson/custvisit",Map("es.mapping.id"->"custid"))   
                                                         dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetranges)
				                                                 println(java.time.LocalTime.now)
				                                                 println("commiting offset")
			                                                   dfchec.show(false)
			                                                   jsondf.printSchema()
			                                                  }
			                                               else
				                                              {println(java.time.LocalTime.now)
				                                                println("No data in the kafka topic for the given iteration")
				                                              }
                                       }
		 }
		 catch
		 {
		                case ex1: java.lang.IllegalArgumentException       => {
							                                                              println("Illegal arg exception")
						                                                              }
        						case ex2: java.lang.ArrayIndexOutOfBoundsException => {
					                                                      		        println("Array index out of bound")
						                                                              }
        						case ex3: org.apache.spark.SparkException          => {
				                                                            			  println("Spark common exception")
				                                                            			}
						        case ex4: java.lang.NullPointerException           => {  
						                                       	                        println("Values Ignored")
						                                       	                      }  
		 }
		 ssc.start()
		 ssc.awaitTermination()
		 println("completed")
		 
	}
}

package org.etl.stage


import org.apache.spark.sql._
import org.apache.spark.streaming.{StreamingContext,Seconds}
import org.apache.kafka.common.serialization.StringDeserializer

object realtimestreaming {
  
  def main (args:Array[String]) {
    
    
    val ss  = new SparkSession.Builder().appName("realtime").master("Local[*}")
                              .enableHiveSupport().getOrCreate()
    val ssm = new StreamingContext(ss.sparkContext,Seconds(1)) 
    
    
    val kafkaparams = Map[String,Object] (
                             "bootstrap.servers"  ->"localhost:9092,localhost:9093,localhost:9094"
                             ,"key.serializers"   ->classOf[StringDeserializer]
                             ,"key.values"        ->classOf[StringDeserializer]
                             ,"group.id"          ->"hackathon"
				                     ,"auto.offset.reset" ->"earliest"
				                     ,"enable.auto.commit"->(false:java.lang.Boolean) 
                          )
    
  }
  
}