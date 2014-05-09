package com.distinct.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.distinct.database.DBConnect;

/**
 * Servlet implementation class HomeQuery
 */
@WebServlet("/HomeQuery")
public class HomeQuery extends HttpServlet {
	private static final long serialVersionUID = 1L;

    /**
     * Default constructor. 
     */
    public HomeQuery() {
        // TODO Auto-generated constructor stub
    	super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		request.setCharacterEncoding("UTF-8");
		response.setContentType("text/html;charset=gb2312");
		
		PrintWriter out = response.getWriter();
		
		String search = request.getParameter("SearchBox");
		
		List<String[]> l = DBConnect.searchByName(search);
		
		if (l != null) {
			out.println("<html><head>");
			out.println("<title>Search Result</title>");
			out.println("</head>");
			out.println("<body><div align=\"center\">");
			
			out.println("<table align='center' border='1'>");
			out.println("<tr><th>Author ID</th><th>Name</th><th>Article Title</th></tr>");
			
			for (int i = 0; i < l.size(); i ++){
				String id = l.get(i)[0];
				String name = l.get(i)[1];
				String title = l.get(i)[3];
				
				out.println("<tr><th>"+id+"</th><th>"+name+"</th><th>"+title+"</th></tr>");
			}
			
			out.println("</table>");
			
			out.println("</div>");
			out.println("</body></html>");
			out.close();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
