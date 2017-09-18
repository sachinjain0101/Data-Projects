package com.peoplenet.main;

public class HeavyWorkRunnable implements Runnable {

	@Override
	public void run() {
		System.out.println("Doing heavy processing - START " + Thread.currentThread().getName());
		try {
			Thread.sleep(1000);
			// Get database connection, delete unused data from DB
			doDBProcessing();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Doing heavy processing - END " + Thread.currentThread().getName());
	}

	private void doDBProcessing() throws InterruptedException {
		System.out.println("Working Hard " + Thread.currentThread().getName());
		Thread.sleep(10000);
	}

}