package com.distinct.database;

import java.io.InputStream;

import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.*;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.distinct.domain.*;


public class Parser extends DefaultHandler{
	
	private Proceeding _proc = null;
	private Publication _pub = null;
	private String _tag = null;
	private boolean _isProc = false;
	private boolean _isPub = false;
	
	@Override
	public void startElement(String uri, String localName, 
			String qName, Attributes attributes) throws SAXException {
		if (qName.equalsIgnoreCase("proceedings")
				|| qName.equalsIgnoreCase("book")
				|| qName.equalsIgnoreCase("incollection")
				|| qName.equalsIgnoreCase("phdthesis")
				|| qName.equalsIgnoreCase("masterthesis")){
			String key = attributes.getValue("key");
			_proc = new Proceeding(key);
			_isProc = true;
			_tag = qName;
		} else if (qName.equalsIgnoreCase("article")
				|| qName.equalsIgnoreCase("inproceedings")) {
			String key = attributes.getValue("key");
			String mdate = attributes.getValue("mdate");
			_pub = new Publication(key);
			_isPub = true;
			_tag = qName;
		} else
			_tag = qName;
	}
	
	@Override
	public void endElement(String uri, String localName,
			String qName) throws SAXException {
		if (qName.equalsIgnoreCase("proceedings")
				|| qName.equalsIgnoreCase("book")
				|| qName.equalsIgnoreCase("incollection")
				|| qName.equalsIgnoreCase("phdthesis")
				|| qName.equalsIgnoreCase("masterthesis")) {
			DBConnect.insertProc(_proc);
//			_proc.print();
			_proc = null;
			_isProc = false;
		} else if (qName.equalsIgnoreCase("article")
				|| qName.equalsIgnoreCase("inproceedings")) {
			DBConnect.insertPub(_pub);
//			_pub.print();
			_pub = null;
			_isPub = false;
		}
		_tag = null;
	}
	
	@Override
	public void characters(char ch[], int start, int length)
			throws SAXException {
		if (_tag != null) {
			if (_isProc) {
				if (_tag.equalsIgnoreCase("year"))
					this._proc.setYear(new String(ch, start, length));
				else if (_tag.equalsIgnoreCase("address"))
					this._proc.setLocation(new String(ch, start, length));
				else if (_tag.equalsIgnoreCase("publisher"))
					this._proc.setPublisher(new String(ch, start, length));
			} else if (_isPub) {
				if (_tag.equalsIgnoreCase("title")){
					this._pub.setTitle(new String(ch, start, length));
				} else if (_tag.equalsIgnoreCase("author")) {
					Author auth = new Author(new String(ch, start, length));
					_pub.addAuthor(auth);
				} else if (_tag.equalsIgnoreCase("crossref")) {
					this._pub.setProcKey(new String(ch, start, length));
				}
			}
		}
	}
}
