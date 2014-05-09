package com.distinct.database;

import java.io.File;
import java.io.IOException;
import java.util.List;
 
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
 
import org.xml.sax.SAXException;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		try {
			SAXParser p = saxParserFactory.newSAXParser();
			Parser handler = new Parser();
			File f = new File("E:\\LuoYunkai\\SJTU\\Thesis\\DBLP\\dblp.xml");
			
			p.parse(f, handler);
		} catch (ParserConfigurationException | SAXException | IOException e) {
			e.printStackTrace();
		}
	}

}
