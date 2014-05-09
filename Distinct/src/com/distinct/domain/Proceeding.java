package com.distinct.domain;

import java.util.List;

public class Proceeding extends Node{
	private String _key;
	private String _year;
	private String _location;
	private String _publisher;
	
	public Proceeding(String key) {
		// TODO Auto-generated constructor stub
		super();
		this._key = key;
	}
	
	public Proceeding(String key, String year, String conf){
		super();
		_key = key;
		_year = year;
		_publisher = conf;
	}
	
	public String getKey() {
		return this._key;
	}

	public void setYear(String year) {
		// TODO Auto-generated method stub
		this._year = year;
	}
	
	public String getYear() {
		return this._year;
	}

	public void setLocation(String location) {
		// TODO Auto-generated method stub
		this._location = location;
	}
	
	public String getLocation() {
		return this._location;
	}
	
	public void print(){
		System.out.println(_key);
		System.out.println(_year);
		System.out.println(_location);
		System.out.print("\n");
	}

	public void setPublisher(String publisher) {
		// TODO Auto-generated method stub
		this._publisher = publisher;
	}
	
	public String getPublisher() {
		return this._publisher;
	}

}
