package com.peoplenet.main;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Kicker {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("HelloSpark").setMaster("local[2]")
				// 4 workers
				.set("spark.executor.instances", "4")
				// 5 cores on each workers
				.set("spark.executor.cores", "5");
		;

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<String> rdd = jsc.textFile("C:\\BD-Projects\\sample-data\\Phase-III.csv");

		try {
			doSomething(rdd);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		jsc.stop();
		jsc.close();

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
