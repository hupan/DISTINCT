package com.distinct.algorithm;

import java.util.List;

import com.distinct.domain.Author;

public class ResemTable {
	
	public double[][] resem;
	public double[][] prob;
	
	public List<Author> authors;
	
	public double[][] getSimTable(){
		double[][] ret = new double[resem.length][resem.length];
		for (int i = 0; i < ret.length; i ++){
			for (int j = 0; j < ret.length; j ++){
				ret[i][j] = Math.sqrt(resem[i][j] * prob[i][j]);
			}
		}
		
		return ret;
	}

}
