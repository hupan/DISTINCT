package com.distinct.algorithm;

import java.util.ArrayList;
import java.util.List;

import com.distinct.database.AlgoDB;
import com.distinct.domain.Author;
import com.distinct.domain.Node;
import com.distinct.domain.Proceeding;
import com.distinct.domain.Publication;

public class Similarity {
	
	static public List<JoinPath> paths = new ArrayList<JoinPath>();
	static public List<Author> authors = new ArrayList<Author>();
	static List<Publication> publications = new ArrayList<Publication>();
	static List<Proceeding> proceedings = new ArrayList<Proceeding>();
	
	static int authorNum;
	
	static void loadTuples(){
		AlgoDB.loadTuples();
		authorNum = authors.size();
		
		for (int i = 0; i < authorNum; i ++){
			Author auth = authors.get(i);
			List<Author> temp = new ArrayList<Author>();
			for (int j = 0; j < authorNum; j ++){
				Author a = authors.get(j);
				if (a.getName().equals(auth.getName()))
					temp.add(a);
			}
			
			for (int k = 0; k < temp.size(); k ++){
				Author a = authors.get(k);
				for (int j = 0; j < temp.size(); j ++){
					Author ta = temp.get(j);
					for (int n = 0; n < ta.publications.size(); n ++){
						Publication p = (Publication) ta.publications.get(n);
						
						boolean found = false;
						for (int q = 0; q < a.publications.size(); q ++){
							if (a.publications.get(q).getID().equals(p.getID())){
								found = true;
								break;
							}
						}
						if (!found){
							a.publications.add(p);
							p.authors.add(a);
						}
					}
				}
			}
		}
	}
	
	static public Node haveNode(String key){
		for (int i = 0; i < authors.size(); i ++){
			Author auth = authors.get(i);
			if (auth.getID().equals(key))
				return auth;
			for (int j = 0; j < auth.publications.size(); j ++){
				Publication pub = (Publication) auth.publications.get(j);
				if (pub.getID().equals(key))
					return pub;
				for (int k = 0; k < pub.proceedings.size(); k ++){
					Proceeding proc = (Proceeding) pub.proceedings.get(k);
					if (proc.getID().equals(key))
						return proc;
				}
			}
		}
		return null;
	}
	
	//Create TableNode Graph
	static private List<TableNode> initGraph(){
		List<TableNode> ret = new ArrayList<TableNode>();
		
		TableNode ath = new TableNode("Author");
		TableNode pub = new TableNode("Publication");
		TableNode pro = new TableNode("Proceeding");
		
		ath.add(pub);
		pub.add(ath);
		pub.add(pro);
		pro.add(pub);
		
		ret.add(ath);
		ret.add(pub);
		ret.add(pro);
		
		return ret;
	}
	
	//L >= 2
	static private List<JoinPath> generateJoinPaths(int L){
		//Update paths;
		List<TableNode> g = initGraph();
		if (L == 2){
			List<JoinPath> ret = new ArrayList<JoinPath>();
			JoinPath jp = new JoinPath();
			jp.append("Author");
			jp.append("Publication");
			ret.add(jp);
			return ret;
		} else {
			List<JoinPath> ret = generateJoinPaths(L - 1);
			int size = ret.size();
			for (int i = 0; i < size; i ++){
				if (ret.get(i).getLength() == L - 1){
					String last = ret.get(i).getPath().get(ret.get(i).getPath().size() - 1);
					for (int j = 0; j < g.size(); j ++){
						if (last == g.get(j).getName()){
							TableNode tn = g.get(j);
							for (int k = 0; k < tn.getAgacent().size(); k ++){
								JoinPath n_jp = new JoinPath(ret.get(i));
								n_jp.append(tn.getAgacent().get(k).getName());
								ret.add(n_jp);
							}
						}
					}
				}
			}
			return ret;
		}
	}
	
	static void updatePaths(int L){
		paths = generateJoinPaths(L);
	}
	
	
	static void clearProbs(){
		for (int i = 0; i < authors.size(); i ++)
			authors.get(i).clearProbs();
	}
	
	static public ResemTable computeResem(String name){
		ResemTable rt = new ResemTable();
		List<Author> authorList = new ArrayList<Author>();
		
		for (int i = 0; i < Similarity.authors.size(); i ++){
			if (Similarity.authors.get(i).getName().equals(name)){
				authorList.add(Similarity.authors.get(i));
			}
		}
		
		int size = authorList.size();
		
		double totalWeight = 0;
		
		rt.resem = new double[size][size];
		rt.prob = new double[size][size];
		
		for (int i = 0; i < size; i ++)
			for (int j = 0; j < size; j ++){
				rt.resem[i][j] = 0;
				rt.prob[i][j] = 0;
			}
		
		for (int m = 0; m < paths.size(); m ++){
			JoinPath path = paths.get(m);
			ResemTable temp = path.computeResem(authorList);
			
			for (int i = 0; i < size; i ++){
				for (int j = 0; j < size; j ++){
					rt.resem[i][j] += temp.resem[i][j] * path.weight;
					rt.prob[i][j] += temp.prob[i][j] * path.weight;
				}
			}
			totalWeight += path.weight;
		}
		
		for (int i = 0; i < size; i ++){
			for (int j = 0; j < size; j ++){
				rt.resem[i][j] = rt.resem[i][j] / totalWeight;
				rt.prob[i][j] = rt.prob[i][j] / totalWeight;
			}
		}
		
		rt.authors = authorList;
		
		return rt;
	}

}
