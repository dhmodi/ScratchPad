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

					val md5Value = sc.getConf.get("spark.scdengine.md5ValueColumn");
					val batchId = sc.getConf.get("spark.scdengine.batchIdColumn");
					val currInd = sc.getConf.get("spark.scdengine.currentIndicatorColumn");
					val startDate = sc.getConf.get("spark.scdengine.startDateColumn");
					val endDate = sc.getConf.get("spark.scdengine.endDateColumn");
					val updateDate = sc.getConf.get("spark.scdengine.updateDateColumn");

					val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);
					import sqlContext.implicits._
					var src=sqlContext.sql(s"select * from $srcDatabase.$srcTable");
					val tgt=sqlContext.sql(s"select * from $tgtDatabase.$tgtTable");
					val srcDataTypes = src.dtypes
							val tgtDataTypes = tgt.drop("md5Value").drop("batchId").drop("currInd").drop("startDate").drop("endDate").drop("updateDate").dtypes

							val toInt    = udf[Int, String]( _.toInt);
					val toDouble = udf[Double, String]( _.toDouble);
					val conToString = udf[String, Any]( _.toString);
					val featureSRC = src.select();


					if(srcDataTypes.deep  != tgtDataTypes.deep)
					{
						for ( x <- 0 to (tgtDataTypes.length - 1) ) {
							if (srcDataTypes(x) != tgtDataTypes(x))
							{
								val cols = tgtDataTypes(x).toString().split(",").map(_.trim);
								val columnName = cols(0).replaceAll("[()]","")
										val dataType = cols(1).toString.split("\\(")(0).replaceAll("[)]","")
		 val decPrecision = cols(1).toString.split("\\(")(1).replaceAll("[)]","")
		val decScale = cols(2).replaceAll("[)]","");
		var decFormat = "0";
										 for ( i <- (1 to decPrecision.toInt() - 1){
										   decFormat = decFormat + "0";
										   										 }
										dataType match {
										case "IntegerType"  => { src = src.withColumn(s"$columnName", toInt(src(s"$columnName"))) };
										case "StringType" => { src = src.withColumn(s"$columnName", conToString(src(s"$columnName")))};
										
								}
							}
						}
					}

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
						val md5DF = src.map(r => (r.getAs(s"${tblPrimaryKey}").toString, r.hashCode.toString)).toDF(s"${tblPrimaryKey}",s"$md5Value")
								md5DF.show();
						val newSrc = src.join(md5DF,s"${tblPrimaryKey}");
						newSrc.show();
						var tgtFinal=tgt.filter(s"$currInd" + " = 'N'"); //Add to final table
						tgtFinal.show()
						val tgtActive=tgt.filter(s"$currInd" + " = 'Y'");
						// Check for duplicate in SRC & TGT
						val devSrc = newSrc.except(tgtActive.as('a).join(newSrc.as('b),tgtActive.col(s"$md5Value") === newSrc.col(s"$md5Value")).select("b.*").dropDuplicates());
						devSrc.show()
						val newTgt2 = tgtActive.as('a).join(devSrc.as('b),tgtActive.col(s"${tblPrimaryKey}") === devSrc.col(s"${tblPrimaryKey}"));
						newTgt2.show()
						tgtFinal = tgtFinal.unionAll(tgtActive.except(newTgt2.select("a.*")));
						val inBatchID = udf((t:String) => "13" );
						val inCurrInd = udf((t:String) => "Y" );
						val NCurrInd = udf((t:String) => "N" );
						val fendDate = udf((t:String) => "9999-12-31 23:59:59");
						tgtFinal = tgtFinal.unionAll(newTgt2.select("a.*").withColumn(s"$currInd", NCurrInd(col(s"${tblPrimaryKey}"))).withColumn(s"$endDate", current_timestamp()).withColumn(s"$updateDate", current_timestamp()));
						val srcInsert = devSrc.withColumn(s"$batchId", inBatchID(col(s"${tblPrimaryKey}"))).withColumn(s"$currInd", inCurrInd(col(s"${tblPrimaryKey}"))).withColumn(s"$startDate", current_timestamp()).withColumn(s"$endDate", date_format(fendDate(col(s"${tblPrimaryKey}")),"yyyy-MM-dd HH:mm:ss")).withColumn(s"$updateDate", current_timestamp());
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