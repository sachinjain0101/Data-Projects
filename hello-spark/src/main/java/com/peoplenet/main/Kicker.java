package com.peoplenet.main;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

public class Kicker {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		Map<String,String> dbOpts = DBOps.getDBConnectOptions();
		
		SparkSession spark = SparkSession
						      .builder()
						      .appName("DBConnect")
						      .master("local[5]")
						      .config("spark.executor.instances", "1")
						      .config("spark.executor.cores", "4")
						      .getOrCreate();
		
		
		Dataset<Row> rows = getData(spark);
		
		System.out.println("----------------ds count --- "+rows.count());
		
		System.out.println("---------------------------------------------------");
		rows.show();
		
		System.out.println("---------------------------------------------------");
		rows.printSchema();
		System.out.println("---------------------------------------------------");
		
		
		
		Iterator<Row> itr  = rows.toLocalIterator();
		
		
		
		while(itr.hasNext()) {
			Row r = itr.next();
			System.out.println(r.toString());
		}
		
		spark.close();
		
		
		
		SparkConf sparkConf = new SparkConf().setAppName("HelloSpark").setMaster("local[2]")
				.set("spark.executor.instances", "1") //workers
				.set("spark.executor.cores", "4"); //cores/worker
		;

		
		
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaRDD<String> rdd = jsc.textFile("C:\\BD-Projects\\sample-data\\Phase-III.csv");
		
		Builder session = SparkSession.builder().appName("DBConnect").master("local[3]");
		
		
		try {
			doSomething(rdd);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		jsc.stop();
		jsc.close();

	}
	
	static Dataset<Row> getData(SparkSession spark) {
		
//		Dataset<Row> jdbcDF2 = spark.read().format("jdbc").option("url", "jdbc:postgresql://localhost:5432")
				//.option("dbtable", "pnet_dw.tblsparktest").option("user", "postgres").option("password", "Welcome123#")
				//.load();

		Properties connectionProperties = new Properties();
		connectionProperties.put("driver", "org.postgresql.Driver");
		connectionProperties.put("user", "postgres");
		connectionProperties.put("password", "Welcome123#");
		Dataset<Row> jdbcDF2 = spark.read().jdbc("jdbc:postgresql://localhost:5432/postgres", "pnet_dw.tblsparktest",
				connectionProperties);
		
		return jdbcDF2;
	}

	static void doSomething(JavaRDD<String> rdd) throws InterruptedException {
		ExecutorService executor = Executors.newFixedThreadPool(5);
		for (int i = 1; i <= 5; i++) {
			Runnable worker = new HeavyWorkRunnable();
			executor.execute(worker);
		}

		executor.shutdown();
		executor.awaitTermination(20, TimeUnit.SECONDS);
		System.out.println("Finished all threads");

	}

}
