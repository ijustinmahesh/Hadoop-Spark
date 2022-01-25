#!/usr/bin/python
import logging
import logging.config
import commands
import os
#import datetime
from datetime import datetime
import glob
import subprocess

ginprogress = '\033[1;92m'+"bgprocess"+'\033[0m'
gstatus    = '\033[1;92m'+"Done"+'\033[0m' 
def main():
    print("\n")
    print("  -------------------------------------------  ")
    print(" |     ****Hackathon Excercise****           | ")
    print("  -------------------------------------------  ")
    print(" |  -Execution started                       | ")
    formatter    ="%(asctime)s %(levelname)s %(message)s"
    logger       =logging.getLogger("HackathonApp")
    fh           =logging.FileHandler("hackapp.log")                    #Creating file handler
    formatter    ="%(asctime)s %(levelname)s %(message)s"
    logformat    =logging.Formatter(formatter)
    fh.setFormatter(logformat)
    logger.addHandler(fh)                                               #Attaching fh to logger obj
    logger.setLevel(logging.DEBUG)
    part_abc(logger)
    print(" |  -Execution completed                     | ")
    print("  -------------------------------------------  ")
    print("Note:-Part c is streaming app running in bground")

    print("Note:-Refer the log file hackapp.log")
    print("\n")

def part_abc(logger) :
    logger.info("Starting Part-ABC")
    print(" |     --Running part-ABC                    | ")
    cmd   = "hadoop fs -test -d /user/hduser/sparkhack2"
    (exitcode, op) = commands.getstatusoutput(cmd)
    if int(exitcode) > 0 :
       logger.info("Missing hackapp dir in hdfs so creating it...")
       cmd = "hadoop fs -mkdir /user/hduser/sparkhack2"
       (exitcode, op) = commands.getstatusoutput(cmd)
       if exitcode==0:
          logger.info("Hackapp directory created in hdfs ")
          print(" |        --HDFS Directory  "+gstatus+"             | ")
    else:
       logger.info(" hackapp dir already exists hence skipping creating directory step")
       print(" |         --HDFS Directory  "+gstatus+"            | ")
    
    csv_files = glob.glob(os.path.join("/hackathon_ijm/", '*.csv'))
    if csv_files ==  None:
       raise Exception("Files are not available")
    else:
       now = datetime.now()
       ct = now.strftime("%m%d%Y_%H%M%S")
 
       filecsv=""
       files  =""
       if len(csv_files)==3:
          for files in csv_files :
              filecsv =  os.path.basename(files)+"_"+str(ct)
              cmd = "hadoop fs -copyFromLocal -f "+files+" /user/hduser/sparkhack2/"+filecsv
              (exitcode, op) = commands.getstatusoutput(cmd)
              logger.info("file %s is copied to hdfs" %filecsv)
              logger.info(cmd)
       else:
            raise Exception("Missing Files and available files are " + " ,".join(csv_files) )
       print(" |        --Files to HDFS   "+gstatus+"             | ")
       cmd="spark-submit   --class  org.etl.stage.parta  sparkstreaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar  --driver-memory 512 --number-of-executors 1 --executor-cores 2"
       (exitcode, op) = commands.getstatusoutput(cmd)
       logger.info(" #####################PART A #######################################################")
       logger.info(op)
       logger.info(" #####################PART A completed #############################################")
       if exitcode==0:
          print(" |        --parta           "+gstatus+"             | ")
          cmd="spark-submit  --class  org.etl.stage.partb sparkstreaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
                                      --driver-memory 512 --number-of-executors 1 --executor-cores 2"
          (exitcode, op) = commands.getstatusoutput(cmd)
          if exitcode==0:
             logger.info(" #####################PART B #################################################")
             logger.info(op)
             logger.info(" #####################PART B Completed #######################################")
             print(" |        --partb           "+gstatus+"             | ")
             cmd=" nohup spark-submit  --class  org.etl.stage.partc  sparkstreaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar\
                                      --driver-memory 512 --number-of-executors 1 --executor-cores 2 > hackapp.log 2>&1"
             process = subprocess.Popen(cmd, shell=True, stdout=None, stderr=None,stdin=None,close_fds=True)
             #my_pid, err = process.communicate()
             #(exitcode, op) = commands.getstatusoutput(cmd)
             logger.info(op)
             print(" |        --partc           "+ginprogress+"       | ")

if __name__=='__main__' :
   main()
