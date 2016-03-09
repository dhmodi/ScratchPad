package com.scd.engine

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe
//import org.apache.spark.sql.sqlContext.implicits._


object SparkSCDEngine {
	def main(args: Array[String]) {
		if (args.length < 2) {
			System.err.println("Usage: SparkSCDEngine <host> <config_file>")
			System.exit(1)
		}
		//System.setProperty("hadoop.home.dir", "C:/Users/dhmodi/Downloads/hadoop-common-2.2.0-bin-master/hadoop-common-2.2.0-bin-master")

		val bufferedSource = scala.io.Source.fromFile(args(1))
				for (line <- bufferedSource.getLines) {
					val cols = line.split(",").map(_.trim);
					val srcDatabase = cols(0);
					val srcTable = cols(1);
					val tgtDatabase = cols(2);
					val tgtTable = cols(3);
					val tblPrimaryKey = cols(4);
					val scdType = cols(5);
					val loadType = cols(6);

					//					println(tgtTable)
					//					println(srcTable)
					//					println(tblPrimaryKey)

					val conf = new SparkConf().setAppName("SparkSCDEngine").setMaster(args(0));
					val sc = new SparkContext(conf);

					val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);
					import sqlContext.implicits._
					val src=sqlContext.sql(s"select * from $srcDatabase.$srcTable");
					val tgt=sqlContext.sql(s"select * from $tgtDatabase.$tgtTable");

					scdType match {
					case "Type1" => {
						println("SCD Type1: Unimplemented");
						val newTgt1 = tgt.as('a).join(src.as('b),tgt.col(s"$tblPrimaryKey") === src.col(s"$tblPrimaryKey"));
						var tgtFinal = tgt.except(newTgt1.select("a.*"));
						tgtFinal = tgtFinal.unionAll(src);
						tgtFinal.write.mode(SaveMode.Append).saveAsTable(s"$tgtDatabase.$tgtTable");

					}
					case "Type2" => {

						// SCD Type 2 
						val md5DF = src.map(r => (r.getAs(s"${tblPrimaryKey}").toString, r.hashCode.toString)).toDF(s"${tblPrimaryKey}","md5Value")
						md5DF.show();
						val newSrc = src.join(md5DF,s"${tblPrimaryKey}");
						newSrc.show();
						var tgtFinal=tgt.filter("currind = 'N'"); //Add to final table
						val tgtActive=tgt.filter("currind = 'Y'");
						// Check for duplicate in SRC & TGT
						val devSrc = newSrc.except(tgtActive.as('a).join(newSrc.as('b),tgtActive.col("md5Value") === newSrc.col("md5Value")).select("b.*").dropDuplicates());
						val newTgt2 = tgtActive.as('a).join(devSrc.as('b),tgtActive.col(s"${tblPrimaryKey}") === devSrc.col(s"${tblPrimaryKey}"));
						tgtFinal = tgtFinal.unionAll(tgtActive.except(newTgt2.select("a.*")));
						val inBatchID = udf((t:String) => "13" );
						val inCurrInd = udf((t:String) => "Y" );
						val NCurrInd = udf((t:String) => "N" );
						val endDate = udf((t:String) => "9999-12-31 23:59:59");
						tgtFinal = tgtFinal.unionAll(newTgt2.select("a.*").withColumn("currInd", NCurrInd(col(s"${tblPrimaryKey}"))).withColumn("endDate", current_timestamp()).withColumn("updateDate", current_timestamp()));
						val srcInsert = devSrc.withColumn("batchId", inBatchID(col(s"${tblPrimaryKey}"))).withColumn("currInd", inCurrInd(col(s"${tblPrimaryKey}"))).withColumn("startDate", current_timestamp()).withColumn("endDate", date_format(endDate(col(s"${tblPrimaryKey}")),"yyyy-MM-dd HH:mm:ss")).withColumn("updateDate", current_timestamp());
						tgtFinal = tgtFinal.unionAll(srcInsert);
						// tgtFinal.write.mode(SaveMode.Append).saveAsTable(s"$tgtDatabase.tgt_table2");
						tgtFinal.registerTempTable(s"$tgtTable"+"_tmp")
						sqlContext.sql(s"insert overwrite table $tgtTable select * from $tgtTable" + "_tmp");
					}
					// catch the default with a variable so you can print it
					case whoa  => println("Unexpected case: " + whoa.toString);
					}
				}
		System.exit(0)
	}

}