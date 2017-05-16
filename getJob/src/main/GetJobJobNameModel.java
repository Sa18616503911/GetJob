package main;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import bean.ConnBean;
import utils.ConnectionUtil;
import utils.GetJobUtils;

public class GetJobJobNameModel {

	private static Properties properties = new Properties();
	// 原始链接
	private static Connection con = null;
	private static Statement stmt = null;
	private static final String bgs = "(";
	private static final String eds = ")";
	private static final String sp = ",";
	private static final Integer maxSubLine = 100; // 最大单次提交sql行
	private static final String lip = "'";
	private static  String dbConfigId = "";
	
	private static void main(String[] args) throws Exception{
		dbConfigId = "";
		HashMap<String, String> tabListMap = new HashMap<String, String>(); //
		InputStream in = new FileInputStream("E:/dttest/config1.properties");
		properties.load(in);
		getSrcConnection(); // 初始化链接
		GetJobUtils jobUtils = new GetJobUtils("`hbasekey_bk`", "`jobinfo_bk`");
		
		ArrayList<ConnBean> connBeans = jobUtils.getDbconfigList(stmt, dbConfigId);
		
		
		closeSrcConnection() ;
	}
	
	private static void getSrcConnection() throws Exception {
		con = ConnectionUtil.getConnent(properties.getProperty("url"), properties.getProperty("user"),
				properties.getProperty("password"));
		stmt = con.createStatement();
	}
	
	private static void closeSrcConnection() throws Exception {
		stmt.close();
		con.close();
	}
	
	private static void getJobList(ArrayList<ConnBean> connBeans, HashMap<String, String> tabListMap) throws Exception {
		Connection cons = null;
		Statement stmts = null;
		for (ConnBean cb : connBeans) {
			 cons = ConnectionUtil.getConnentByClass(cb.getUrl(), cb.getUsname(), cb.getPassword(),
					cb.getClassName());
			 stmts = cons.createStatement();
			if (cb.getClassName().equals("com.mysql.jdbc.Driver")) {
				//mySqlGetKey(stmts, dbName);
			}else {
				//oracleGetKey(stmts, dbName);
			}
			cons.close();
			stmts.close();
		}
	}
	
	
	
	
}
