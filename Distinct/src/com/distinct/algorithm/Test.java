package com.distinct.algorithm;

import java.util.ArrayList;
import java.util.List;

import com.distinct.domain.Author;
import com.distinct.domain.Node;
import com.distinct.domain.Proceeding;
import com.distinct.domain.Publication;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Similarity.updatePaths(3);
		
		for (int i = 0; i < Similarity.paths.size(); i ++){
			JoinPath jp = Similarity.paths.get(i);
			for (int j = 0; j < jp.getPath().size(); j ++){
				System.out.print(jp.getPath().get(j) + "\t");
			}
			System.out.println();
		}
//		
//		Author a1, a2, a3;
//		a1 = new Author("Jack", 1);
//		a2 = new Author("Jack", 2);
//		a3 = new Author("Jack", 3);
//		
//		Publication p1, p2, p3;
//		p1 = new Publication("p1");
//		p2 = new Publication("p2");
//		p3 = new Publication("p3");
//		
//		Proceeding pr1, pr2;
//		pr1 = new Proceeding("pr1");
//		pr2 = new Proceeding("pr2");
//		
//		a1.publications.add(p1);
//		a1.publications.add(p3);
//		a2.publications.add(p3);
//		a3.publications.add(p1);
//		a3.publications.add(p2);
//		a3.publications.add(p3);
//		
//		p1.authors.add(a1);
//		p1.authors.add(a3);
//		p2.authors.add(a3);
//		p3.authors.add(a1);
//		p3.authors.add(a2);
//		p3.authors.add(a3);
//		
//		p1.proceedings.add(pr2);
//		p2.proceedings.add(pr1);
//		p3.proceedings.add(pr1);
//		
//		pr1.publications.add(p2);
//		pr1.publications.add(p3);
//		pr2.publications.add(p1);
//		
//		Similarity.authors.add(a1);
//		Similarity.authors.add(a2);
//		Similarity.authors.add(a3);
//		
//		for (int i = 0; i < Similarity.paths.size(); i ++){
//			JoinPath jp = Similarity.paths.get(i);
//			jp.computeResem();
//			for (int m = 0; m < 3; m ++){
//				for (int n = 0; n < 3; n ++){
//					System.out.print(jp.resemTable[m][n]);
//					System.out.print("\t");
//				}
//				System.out.println("\n");
//			}
//		}
		
//		List<Tuple> lt = Similarity.paths.get(1).propagate(a1);
//		for (int i = 0; i < lt.size(); i ++){
//			Tuple t = lt.get(i);
//			System.out.print(t.key + "\t" + t.prob1 + "\t" + t.prob2);
//			System.out.println();
//		}
		
		Similarity.loadTuples();
//		for (int i = 0; i < Similarity.paths.size(); i ++){
//			JoinPath path = Similarity.paths.get(i);
//			for (int j = 0; j < Similarity.authors.size(); j ++){
//				Author auth = Similarity.authors.get(j);
//				if (auth.getName().equals("Liang Zhao")){
//					path
//				}
//			}
//		}
		
		System.out.println(Similarity.authors.size());
//		for (int i = 0; i < Similarity.authorNum; i ++){
//			Author auth = Similarity.authors.get(i);
//			System.out.println(auth.getName() + "\t" + auth.publications.size());
//		}
//		
		
//		List<Node> listNode = new ArrayList<Node>();
//		for (int i = 0; i < Similarity.authorNum; i ++){
//			Author auth = Similarity.authors.get(i);
//			
//			if (auth.getID().equals("220") || auth.getID().equals("574")){
//				System.out.print(auth.getName());
//				listNode.add(auth);
//				Publication pub = (Publication) auth.publications.get(0);
//				for (int k = 0; k < pub.authors.size(); k ++){
//					Author a = (Author) pub.authors.get(k);
//					System.out.print("\t" + a.getName());
//				}
//				System.out.println();
//				
//				JoinPath jp = Similarity.paths.get(4);
//				List<Tuple> tb = jp.propagate(auth);
//				
//				for (int n = 0; n < tb.size(); n ++){
//					Tuple t = tb.get(n);
//					System.out.print(t.key + "\t" + t.prob1 + "\t" + t.prob2);
//					System.out.println();
//				}
//				System.out.println();
//				
//			}
//			
//		}
//		System.out.println();
//		JoinPath jp = Similarity.paths.get(4);
//		double[][] table = jp.computeResem(listNode);
//		
//		System.out.println();
//		System.out.println(table[0][0] + "\t" + table[0][1]);
//		System.out.println(table[1][0] + "\t" + table[1][1]);
		
		ResemTable matrix = Similarity.computeResem("Liang Zhao");
		for (int i = 0; i < matrix.resem.length; i ++){
			for (int j = 0; j < matrix.resem.length; j ++){
				System.out.print(matrix.resem[i][j]);
				System.out.print("\t");
			}
			System.out.println();
		}
		
		System.out.println();
		
		for (int i = 0; i < matrix.resem.length; i ++){
			for (int j = 0; j < matrix.resem.length; j ++){
				System.out.print(matrix.prob[i][j]);
				System.out.print("\t");
			}
			System.out.println();
		}
		
		System.out.println();
		
		List<Cluster> lc = Clustering.cluster(matrix);
		for (int i = 0; i < lc.size(); i ++){
			Cluster c = lc.get(i);
			for (int k = 0; k < c.getSize(); k ++){
				Author a = c.get(k);
				System.out.print("\t" + a.getName() + a.getID());
			}
			System.out.println();
		}
	}

}
