package com.distinct.algorithm;

import java.util.ArrayList;
import java.util.List;

import com.distinct.domain.Author;

public class Cluster {
	private List<Author> authors;
	
	public Cluster(){
		authors = new ArrayList<Author>();
	}
	
	public void add(Author auth){
		authors.add(auth);
	}
	
	public Cluster merge(Cluster c){
		for (int i = 0; i < c.authors.size(); i ++){
			authors.add(c.authors.get(i));
		}
		
		return this;
	}
	
	public Author get(int i){
		return authors.get(i);
	}
	
	public int getSize(){
		return authors.size();
	}
}
