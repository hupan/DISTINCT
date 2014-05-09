package com.distinct.algorithm;

import java.util.ArrayList;
import java.util.List;

public class Clustering {
	static double threshold = 0.5;
	
	static public List<Cluster> cluster(ResemTable rt){
		List<Cluster> lc = new ArrayList<Cluster>();
		
		//Initiate
		for (int i = 0; i < rt.authors.size(); i ++){
			Cluster c = new Cluster();
			c.add(rt.authors.get(i));
			lc.add(c);
		}
		
		//HAC
		boolean stop = false;
		
		while (!stop){
			
			int[] index = getBiggestSimIndex(rt.getSimTable());
			if (rt.getSimTable()[index[0]][index[1]] <= threshold){
				stop = true;
				break;
			}
			
			merge(index[0], index[1], lc, rt);
		}
		
		return lc;
	}
	
	static int[] getBiggestSimIndex(double[][] sim){
		int[] index = new int[2];
		
		index[0] = 0;
		index[1] = 0;
		
		for (int i = 0; i < sim.length; i ++){
			for (int j = 0; j < sim.length && j != i; j ++){
				if (sim[i][j] >= sim[index[0]][index[1]]){
					index[0] = i;
					index[1] = j;
				}
			}
		}
		
		
		return index;
	}
	
	
	
	static void merge(int i, int j, List<Cluster> lc, ResemTable lastrt) {
		Cluster ci = lc.remove(i);
		Cluster cj = lc.remove(j);
		Cluster c = ci.merge(cj);
		lc.add(c);
				
		double[][] resem = new double[lc.size()][lc.size()];
		double[][] prob = new double[lc.size()][lc.size()];
		
		for (int m = 0; m < lc.size() - 2; m ++){
			for (int n = 0; n < lc.size() - 2; n ++){
				int a = m;
				int b = n;
				
				int s = (i > j) ? j : i;
				int l = (i > j) ? i : j;
				
				if (a >= l - 1) a += 2;
				else if (a >= s && a < l - 1) a ++;
				
				if (b >= l - 1) b += 2;
				else if (b >= s && b < l - 1) b ++;
				
				resem[m][n] = lastrt.resem[a][b];
				prob[m][n] = lastrt.prob[a][b];
			}
		}
		
		for (int k = 0; k < lc.size(); k ++) {
			resem[lc.size() - 1][k] = (ci.getSize() * lastrt.resem[i][k] + cj.getSize() * lastrt.resem[j][k]) / (ci.getSize() + cj.getSize());
			resem[k][lc.size() - 1] = (ci.getSize() * lastrt.resem[k][i] + cj.getSize() * lastrt.resem[k][j]) / (ci.getSize() + cj.getSize());
			prob[lc.size() - 1][k] = (ci.getSize() * lastrt.prob[i][k] + cj.getSize() * lastrt.prob[j][k]) / (ci.getSize() + cj.getSize());
			prob[k][lc.size() - 1] = (ci.getSize() * lastrt.prob[k][i] + cj.getSize() * lastrt.prob[k][j]) / (ci.getSize() + cj.getSize());
		}
		
		for (int k = 0; k < lc.size(); k ++){
			resem[k][k] = 0;
			prob[k][k] = 0;
		}
		
		lastrt.resem = resem;
		lastrt.prob = prob;
	}

}
