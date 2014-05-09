package com.distinct.database;

import java.sql.*;
import com.mysql.jdbc.Driver;
import java.util.ArrayList;
import java.util.List;

import com.distinct.domain.Author;
import com.distinct.domain.Proceeding;
import com.distinct.domain.Publication;

public class DBConnect {
	static protected Connection conn = null;
	static protected Statement stmt = null;
	static protected PreparedStatement pstmt = null;
	static protected ResultSet rs = null;
	
	static void connect() {
		try {
			if (conn == null || conn.isClosed()){
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/distinct?" + 
						"user=root&password=luoyunkai");
			}
		} catch (SQLException e){
			System.out.println(e.getMessage());
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static void insertProc(Proceeding proc){
		try {
			connect();
			String insql = "INSERT INTO Proceedings"
						 + "(proc_key, conference, year, location) VALUES"
						 + "(?, ?, ?, ?)";
			pstmt = conn.prepareStatement(insql);
			pstmt.setString(1, proc.getKey());
			pstmt.setString(2, proc.getPublisher());
			pstmt.setString(3, proc.getYear());
			pstmt.setString(4, proc.getLocation());
			
//			if (proc.getPublisher() != null 
//					&& stmt.executeQuery("SELECT conference FROM Conferences WHERE publisher="+proc.getPublisher()+"'") == null) {
//				String sql = "INSERT INTO Conferences"
//						 + "(conference, publisher) VALUES"
//						 + "(?, ?)";
//				PreparedStatement pstmt2 = conn.prepareStatement(sql);
//				pstmt2.setString(1, proc.getPublisher());
//				pstmt2.setString(2, proc.getPublisher());
//				pstmt2.executeUpdate();
//			}
			
			pstmt.executeUpdate();
			
		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}
	}
	
	static void insertPub(Publication pub){
		try {
			connect();
			
			String insql = "INSERT INTO Publications"
					 + "(paper_key, title, proc_key) VALUES"
					 + "(?, ?, ?)";
			pstmt = conn.prepareStatement(insql);
			pstmt.setString(1, pub.getKey());
			pstmt.setString(2, pub.getTitle());
			pstmt.setString(3, pub.getProcKey());
			
			pstmt.executeUpdate();
			
			for (int i = 0; i < pub.getAuthorList().size(); i ++) {
				insertAuthor(pub.getAuthorList().get(i));
				String sql = "INSERT INTO Publish"
						 + "(author_key, paper_key) VALUES"
						 + "(?, ?)";
				PreparedStatement pstmt2 = conn.prepareStatement(sql);
				pstmt2.setInt(1, pub.getAuthorList().get(i).getKey());
				pstmt2.setString(2, pub.getKey());
				pstmt2.executeUpdate();
			}
			
		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}
	}
	
	static void insertAuthor(Author auth) {
		try {
			connect();
			String insql = "INSERT INTO Author"
						 + "(author_key, author_name) VALUES"
						 + "(null, ?)";
			pstmt = conn.prepareStatement(insql);
			pstmt.setString(1, auth.getName());
			
			pstmt.executeUpdate();
			
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SHOW TABLE STATUS LIKE 'author'");
			rs.next();
			auth.setKey(rs.getInt("Auto_increment"));
			
		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}
	}
	
	public static List<String[]> searchByName(String s) {
		try {
			connect();
			String query = "SELECT T.author_key, T.author_name, T.paper_key, S.title "
					+ "from (select a.author_key, a.author_name, b.paper_key "
					+ "from ((select * from author where author_name=?) a "
					+ "inner join publish b on a.author_key=b.author_key) "
					+ "order by a.author_key) T left join publications S "
					+ "on T.paper_key=S.paper_key";
			pstmt = conn.prepareStatement(query);
			pstmt.setString(1, s);
			rs = pstmt.executeQuery();
			
			List<String[]> r = new ArrayList<String[]>();
			rs.next();
			while (!rs.isLast()) {
				String[] temp = {Integer.toString(rs.getInt("author_key")), rs.getString("author_name"),
						rs.getString("paper_key"), rs.getString("title")};
				r.add(temp);
				rs.next();
			}
			return r;
		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
			return null;
		} 
	}

}
