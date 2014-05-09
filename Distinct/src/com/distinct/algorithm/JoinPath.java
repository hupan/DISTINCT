package com.distinct.algorithm;

import java.util.ArrayList;
import java.util.List;

import com.distinct.domain.Author;
import com.distinct.domain.Node;
import com.distinct.domain.Proceeding;
import com.distinct.domain.Publication;

public class JoinPath {
	public double weight;
	private List<String> _path;
	
	public double[][] resemTable;
	
	JoinPath(){
		weight = 1;
		_path = new ArrayList<String>();
	}
	
	JoinPath(JoinPath jp) {
		weight = jp.weight;
		_path = new ArrayList<String>();
		for (int i = 0; i < jp.getPath().size(); i ++)
			_path.add(jp.getPath().get(i));
	}
	
	public int getLength(){
		return _path.size();
	}
	
	public void appendTableNode(TableNode tn){
		_path.add(tn.getName());
	}
	
	public void append(String s){
		_path.add(s);
	}
	
	public List<String> getPath(){
		return _path;
	}
	
	private void moveList(List<Node> l1, List<Node> l2){
		l2.clear();
		for (int i = 0; i < l1.size(); i ++)
			l2.add(l1.get(i));
		
		l1.clear();
	}
	
//	private boolean equal(Node n1, Node n2){
//		boolean ret = false;
//		String type = _path.get(_path.size() - 1);
//		
//		switch (type){
//		case "Author":
//			Author a1 = (Author) n1;
//			Author a2 = (Author) n2;
//			ret = (a1.getKey() == a2.getKey());
//			break;
//		case "Publication":
//			Publication p1 = (Publication) n1;
//			Publication p2 = (Publication) n2;
//			ret = (p1.getKey() == p2.getKey());
//			break;
//		case "Proceeding":
//			Proceeding pr1 = (Proceeding) n1;
//			Proceeding pr2 = (Proceeding) n2;
//			ret = (pr1.getKey() == pr2.getKey());
//			break;
//		}
//		
//		return ret;
//	}
	
	private double min(double d1, double d2){
		if (d1 <= d2)
			return d1;
		else
			return d2;
	}
	
	private double max(double d1, double d2){
		if (d1 >= d2)
			return d1;
		else
			return d2;
	}
	
	
	private boolean haveElement(List<Node> ln, Node n){
		boolean ret = false;
		for (int i = 0; i < ln.size(); i ++){
			if (ln.get(i).getID().equals(n.getID())){
				ret = true;
				break;
			}
		}
		return ret;
	}
	
	//Return Neighbor tuples on join path P
	//with their probabilities...
	public List<Tuple> propagate(Node auth){
		
		List<Node> ret = new ArrayList<Node>();
		
		List<Node> temp = new ArrayList<Node>();
		ret.add(auth);
		auth.prob1 = 1;
		auth.prob2 = 1;
		
		if (_path.size() > 1) {
			for (int i = 1; i < _path.size(); i ++){
				String table = _path.get(i);
				switch (table){
				case "Author":
					moveList(ret, temp);
					for (int j = 0; j < temp.size(); j ++){
						for (int k = 0; k < temp.get(j).authors.size(); k ++){
							Node n = temp.get(j).authors.get(k);
							if (!haveElement(ret, n))
								ret.add(n);
						}
					}
					
					for (int j = 0; j < ret.size(); j ++){
						ret.get(j).prob1 = 0;
						ret.get(j).prob2 = 0;
					}
					
					for (int j = 0; j < ret.size(); j ++){
						if (_path.get(i - 1) == "Publication"){
							for (int k = 0; k < ret.get(j).publications.size(); k ++){
								ret.get(j).prob1 += ret.get(j).publications.get(k).prob1 / ret.get(j).publications.get(k).authors.size();
								ret.get(j).prob2 += ret.get(j).publications.get(k).prob2 / ret.get(j).publications.size();
							}
						} else if (_path.get(i - 1) == "Proceeding"){
							for (int k = 0; k < ret.get(j).proceedings.size(); k ++){
								ret.get(j).prob1 += ret.get(j).proceedings.get(k).prob1 / ret.get(j).proceedings.get(k).publications.size();
								ret.get(j).prob2 += ret.get(j).proceedings.get(k).prob2 / ret.get(j).proceedings.size();
							}
						}
					}
					break;
				case "Publication":
					moveList(ret, temp);
					for (int j = 0; j < temp.size(); j ++){
						for (int k = 0; k < temp.get(j).publications.size(); k ++){
							Node n = temp.get(j).publications.get(k);
							if (!haveElement(ret, n))
								ret.add(n);
						}
					}
					
					for (int j = 0; j < ret.size(); j ++){
						ret.get(j).prob1 = 0;
						ret.get(j).prob2 = 0;
					}
					
					for (int j = 0; j < ret.size(); j ++){
						Node p = ret.get(j);
						if (_path.get(i - 1) == "Author"){
							for (int k = 0; k < ret.get(j).authors.size(); k ++){
								p.prob1 += p.authors.get(k).prob1 / p.authors.get(k).publications.size();
								p.prob2 += p.authors.get(k).prob2 / p.authors.size();
							}
						} else if (_path.get(i - 1) == "Proceeding"){
							for (int k = 0; k < ret.get(j).proceedings.size(); k ++){
								p.prob1 = p.prob1 + p.proceedings.get(k).prob1 / p.proceedings.get(k).publications.size();
								p.prob2 = ret.get(j).prob2 + p.proceedings.get(k).prob2 / p.proceedings.size();
							}
						}
					}
					break;
				case "Proceeding":
					moveList(ret, temp);
					for (int j = 0; j < temp.size(); j ++){
						for (int k = 0; k < temp.get(j).proceedings.size(); k ++){
							Node n = temp.get(j).proceedings.get(k);
							if (!haveElement(ret, n))
								ret.add(n);
						}
					}
					
					for (int j = 0; j < ret.size(); j ++){
						ret.get(j).prob1 = 0;
						ret.get(j).prob2 = 0;
					}
					
					for (int j = 0; j < ret.size(); j ++){
						if (_path.get(i - 1) == "Author"){
							for (int k = 0; k < ret.get(j).authors.size(); k ++){
								ret.get(j).prob1 += ret.get(j).authors.get(k).prob1 / ret.get(j).authors.get(k).publications.size();
								ret.get(j).prob2 += ret.get(j).authors.get(k).prob2 / ret.get(j).authors.size();
							}
						} else if (_path.get(i - 1) == "Publication"){
							for (int k = 0; k < ret.get(j).publications.size(); k ++){
								ret.get(j).prob1 += ret.get(j).publications.get(k).prob1 / ret.get(j).publications.get(k).proceedings.size();
								ret.get(j).prob2 += ret.get(j).publications.get(k).prob2 / ret.get(j).publications.size();
							}
						}
					}
					break;
				}
			}
		}
		
		List<Tuple> tl = new ArrayList<Tuple>();
		for (int i = 0; i < ret.size(); i ++){
			Node n = ret.get(i);
			tl.add(new Tuple(n.getID(), n.prob1, n.prob2));
		}
		
		//clear probs
		Similarity.clearProbs();
		
		return tl;
	}
	
	//Update resem matrix for each two tuples
	public ResemTable computeResem(List<Author> ln){
		
		ResemTable rt = new ResemTable();
		
		int size = ln.size();
		rt.resem = new double[size][size];
		rt.prob = new double[size][size];
		
		for (int i = 0; i < size; i ++)
			for (int j = 0; j < size; j ++){
				rt.resem[i][j] = 0;
				rt.prob[i][j] = 0;
			}
		
		for (int i = 0; i < size; i ++)
			for (int j = 0; j < size; j ++){

				List<Tuple> NBPr1 = propagate(ln.get(i));
				List<Tuple> NBPr2 = propagate(ln.get(j));
				
				double numerator = 0;
				double denominator = 0;
				
				double probability = 0;
				
				int count = 0;
				
				for (int m = 0; m < NBPr1.size(); m ++){
					boolean found = false;
					for (int n = 0; n < NBPr2.size(); n ++){
						if (NBPr1.get(m).key.equals(NBPr2.get(n).key)){
							count ++;
							numerator += min(NBPr1.get(m).prob1, NBPr2.get(n).prob1);
							denominator += max(NBPr1.get(m).prob1, NBPr2.get(n).prob1);
							
							probability += NBPr1.get(m).prob1 * NBPr2.get(n).prob2;
							
							found = true;
							break;
						}
					}
					if (!found)
						denominator += NBPr1.get(m).prob1;
				}
				
				if (count != 0){
					probability = probability / count;
				}
				
				for (int n = 0; n < NBPr2.size(); n ++){
					boolean found = false;
					for (int m = 0; m < NBPr1.size(); m ++){
						if (NBPr1.get(m).key.equals(NBPr2.get(n).key)){
							found = true;
							break;
						}
					}
					if (!found)
						denominator += NBPr2.get(n).prob1;
				}
				
				rt.resem[i][j] = numerator / denominator;
				rt.prob[i][j] = probability;
			}
		
		
		for (int i = 0; i < size; i ++){
			rt.resem[i][i] = 0;
			rt.prob[i][i] = 0;
		}
		
		return rt;
	}
}
