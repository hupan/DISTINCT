package com.distinct.domain;

import java.util.ArrayList;
import java.util.List;

public class Publication extends Node{
	private String _key;
	private List<Author> _authorList;
	private String _title;
	private String _procKey;
	
	
	
	public Publication(String key){
		super();
		_authorList = new ArrayList<Author>();
		this._key = key;
	}
	
	public Publication(String key, String title){
		super();
		_key = key;
		_title = title;
	}
	
	public List<Author> getAuthorList() {
		return this._authorList;
	}

	public String getKey() {
		return this._key;
	}
	
	public String getTitle() {
		return _title;
	}

	public void setTitle(String title) {
		this._title = title;
	}
	
	public void addAuthor(Author auth) {
		this._authorList.add(auth);
	}
	
	public void setProcKey(String procKey) {
		this._procKey = procKey;
	}
	
	public String getProcKey() {
		return this._procKey;
	}
	
	public void print(){
		System.out.println(this._key);
		System.out.println(this._title);
		for (int i = 0; i < _authorList.size(); i ++)
			System.out.println(this._authorList.get(i).getName());
		System.out.println("\n");
	}

}
