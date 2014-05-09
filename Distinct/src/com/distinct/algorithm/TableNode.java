package com.distinct.algorithm;

import java.util.ArrayList;
import java.util.List;

public class TableNode {
	private String _name;
	private List<TableNode> _agacent;
	private boolean _isvisited;
	
	TableNode(String name){
		setName(name);
		_agacent = new ArrayList<TableNode>();
		_isvisited = false;
	}
	
	public boolean isVisited(){
		return _isvisited;
	}
	
	public void visit(){
		_isvisited = true;
	}
	
	public void clearStaus(){
		_isvisited = false;
	}
	
	public List<TableNode> getAgacent(){
		return _agacent;
	}
	
	public TableNode getUnvisitedChild(){
		for (int i = 0; i < _agacent.size(); i ++){
			if (!_agacent.get(i).isVisited()){
				_agacent.get(i).visit();
				return _agacent.get(i);
			}
		}
		return null;
	}
	
	public void add(TableNode tn){
		_agacent.add(tn);
	}

	public String getName() {
		return _name;
	}

	public void setName(String _name) {
		this._name = _name;
	}
	
}
