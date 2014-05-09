package com.distinct.database;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.distinct.algorithm.Similarity;
import com.distinct.domain.Author;
import com.distinct.domain.Proceeding;
import com.distinct.domain.Publication;

public class AlgoDB extends DBConnect {
	
	public AlgoDB(){
		super();
	}
	
	static public void loadTuples(){
		try {
			connect();
			String query = "SELECT A.author_key, A.author_name, "
					+ "P.paper_key FROM author A LEFT JOIN publish P "
					+ "ON A.author_key = P.author_key";
			pstmt = conn.prepareStatement(query);
			rs = pstmt.executeQuery();
			
			
			rs.next();
			while (!rs.isLast()) {
				Author auth = new Author(rs.getNString("author_name"), rs.getInt("author_key"));
				String pub_key = rs.getString("paper_key");
				Publication pub = (Publication) Similarity.haveNode(pub_key);
				if (pub != null){
					auth.publications.add(pub);
					pub.authors.add(auth);
				} else {
					query = "SELECT * FROM publications where paper_key=?";
					pstmt = conn.prepareStatement(query);
					pstmt.setString(1, pub_key);
					ResultSet temprs = pstmt.executeQuery();
					if (temprs.first()){
						pub = new Publication(temprs.getString("paper_key"), temprs.getString("title"));
						String proc_key = temprs.getString("proc_key");
						
						Proceeding proc = (Proceeding) Similarity.haveNode(proc_key);
						if (proc != null){
							auth.publications.add(pub);
							pub.authors.add(auth);
							pub.proceedings.add(proc);
							proc.publications.add(pub);
						} else {
							query = "SELECT * FROM proceedings where proc_key=?";
							pstmt = conn.prepareStatement(query);
							pstmt.setString(1, proc_key);
							temprs = pstmt.executeQuery();
							if (temprs.first()){
								proc = new Proceeding(temprs.getString("proc_key"),
									temprs.getString("year"), temprs.getString("conference"));
								auth.publications.add(pub);
								pub.authors.add(auth);
								pub.proceedings.add(proc);
								proc.publications.add(pub);
							}
						}
					}
				}
				Similarity.authors.add(auth);
				rs.next();
			}
		} catch (SQLException e) {
			System.out.println("SQLException: " + e.getMessage());
			System.out.println("SQLState: " + e.getSQLState());
			System.out.println("VendorError: " + e.getErrorCode());
		}
	}
}
