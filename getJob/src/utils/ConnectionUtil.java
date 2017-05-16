package utils;
import java.sql.Connection;
import java.sql.DriverManager;
public class ConnectionUtil {
	
	public static Connection getConnentByClass (String url,String user,String password,String classString) throws Exception{
   		Connection con = null;	
		Class.forName(classString);
		con = DriverManager.getConnection(url, user, password);
		return con;
}
	public static Connection getConnent (String url,String user,String password) throws Exception{
   		Connection con = null;	
		Class.forName("com.mysql.jdbc.Driver");
		con = DriverManager.getConnection(url, user, password);
		return con;
}
}
