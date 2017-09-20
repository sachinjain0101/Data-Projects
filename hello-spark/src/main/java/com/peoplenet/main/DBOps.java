package com.peoplenet.main;

import java.util.HashMap;
import java.util.Map;

public class DBOps {
	
	public static Map<String,String> getDBConnectOptions(){
		Map<String,String> opts = new HashMap<String,String>();
		try {
		    Class.forName("org.postgresql.Driver");
		    System.out.println("PostgreSQL JDBC Driver Registered!");
		} catch (ClassNotFoundException e) {
		    e.printStackTrace();
		}
		opts.put("url", "jdbc:postgresql://localhost:5432/dbname?user=postgres&password=Welcome123#");
		opts.put("dbtable", "test");
		opts.put("driver", "org.postgresql.Driver");
		return opts;
	}

}
