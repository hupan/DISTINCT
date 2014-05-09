package com.distinct.domain;

import java.util.List;

public class Author extends Node{
	private String _name;
	private int _key;
	
	
	public Author(String name) {
		super();
		this._name = name;
	}
	
	public Author(String name, int key){
		super();
		_name = name;
		_key = key;
	}
	
	public void setName(String name){
		this._name = name;
	}
	
	public void setKey(int key){
		this._key = key;
	}
	
	public String getName(){
		return this._name;
	}
	
	public int getKey(){
		return this._key;
	}
}
