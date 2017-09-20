package com.peoplenet.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataOps {

	public static Connection getConnection() throws SQLException {

		Connection conn = null;
		// Properties connectionProps = new Properties();
		// connectionProps.put("user", this.userName);
		// connectionProps.put("password", this.password);

		String url = "jdbc:sqlserver://SACHINJ;databaseName=TimeCurrent;integratedSecurity=true";

		conn = DriverManager.getConnection(url);

		System.out.println("Connected to database");
		return conn;
	}

}
