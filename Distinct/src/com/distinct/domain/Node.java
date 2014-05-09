package com.distinct.domain;

import java.util.ArrayList;
import java.util.List;

public class Node {
	protected List<Node> _neighbors;
	
	public double prob1;
	public double prob2;
	
	public List<Node> authors;
	public List<Node> proceedings;
	public List<Node> publications;
	
	public Node(){
		_neighbors = new ArrayList<Node>();
		authors = new ArrayList<Node>();
		proceedings = new ArrayList<Node>();
		publications = new ArrayList<Node>();
		prob1 = 0;
		prob2 = 0;
	}
	
	public void addAuthor(Author auth){
		
	}
	
	public void addPublication(Publication pub){
		
	}
	
	public void addProceeding(Proceeding proc){
		
	}
	
	public String getID(){
		if (this instanceof Author){
			Author a = (Author) this;
			return Integer.toString(a.getKey());
		} else if (this instanceof Publication){
			Publication p = (Publication) this;
			return p.getKey();
		} else if (this instanceof Proceeding){
			Proceeding pr = (Proceeding) this;
			return pr.getKey();
		} else
			return null;
	}
	
	public void clearProbs(){
		prob1 = 0;
		prob2 = 0;
		for (int i = 0; i < publications.size(); i ++){
			Node p = publications.get(i);
			p.prob1 = 0;
			p.prob2 = 0;
			for (int j = 0; j < p.proceedings.size(); j ++){
				Node pr = p.proceedings.get(j);
				pr.prob1 = 0;
				pr.prob2 = 0;
			}
		}
	}
	
	public List<Node> getKthNeighbors(int k) {
		List<Node> rst = new ArrayList<Node>();
		
		if (k == 1) {
			for (int i = 0; i < _neighbors.size(); i ++)
				rst.add(_neighbors.get(i));
		} else {
			for (int i = 0; i < _neighbors.size(); i ++) {
				List<Node> temp = getKthNeighbors(k - 1);
				for (int j = 0; j < temp.size(); j ++)
					rst.add(temp.get(j));
			}
		}
		
		return rst;
	}

}
