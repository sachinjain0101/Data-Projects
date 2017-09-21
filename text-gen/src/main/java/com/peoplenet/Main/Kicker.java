package com.peoplenet.Main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;

public class Kicker {

	public static void main(String[] args) {

		List<String> lines = new LinkedList<String>();
		for(int i = 0; i<10 ;i++) {
			String x = "";
			for (int j = 0; j<i ;j++) {
				x=x+j;
			}
			lines.add(x);
		}
		
		File f =  new File("C:\\Data-Projects\\sample-data\\out.txt");
		try {
			Files.write(f.toPath(), lines);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
