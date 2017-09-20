package com.peoplenet.main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;

import com.peoplenet.service.DataOps;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {
	private static Producer<String, String> producer;

	public SimpleProducer() {
		Properties props = new Properties();

		// Set the broker list for requesting metadata to find the lead broker
		props.put("metadata.broker.list", "localhost:5900");

		// This specifies the serializer class for keys
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		// 1 means the producer receives an acknowledgment once the lead replica
		// has received the data. This option provides better durability as the
		// client waits until the server acknowledges the request as successful.
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	public static void main(String[] args) {

		String topic = "kafkatopic";// (String) args[0];
		String count = "1000000";
		int messageCount = Integer.parseInt(count);
		System.out.println("Topic Name - " + topic);
		System.out.println("Message Count - " + messageCount);

		SimpleProducer simpleProducer = new SimpleProducer();
		JSONArray arr =  simpleProducer.getData();
		simpleProducer.publishMessage(topic, arr);
	}

	public JSONArray getData() {
		Statement stmt = null;
		String qry = "SELECT TOP 10 [RecordID],[Client],[GroupCode],[SiteNo],[SiteName] FROM TimeCurrent..tblSiteNames";

		try {
			Connection conn = DataOps.getConnection();
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(qry);
			
			return convertToJSON(rs);
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
    public static JSONArray convertToJSON(ResultSet resultSet)
            throws Exception {
        JSONArray jsonArray = new JSONArray();
        while (resultSet.next()) {
            int total_rows = resultSet.getMetaData().getColumnCount();
            JSONObject obj = new JSONObject();
            for (int i = 0; i < total_rows; i++) {
                obj.put(resultSet.getMetaData().getColumnLabel(i + 1)
                        .toLowerCase(), resultSet.getObject(i + 1));
            }
            jsonArray.put(obj);
        }
        return jsonArray;
    }

	private void publishMessage(String topic, JSONArray arr) {
		
		for (int i=0; i< arr.length(); i++){
			String runtime = new Date().toString();
			String json = arr.get(i).toString();
			String msg = "Message Publishing Time - " + runtime + " : " + json;
			System.out.println(msg);
			// Creates a KeyedMessage instance
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);

			// Publish the message
			producer.send(data);
		}
		// Close producer connection with broker.
		producer.close();
	}
}