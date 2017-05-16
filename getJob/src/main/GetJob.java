package main;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import bean.ConnBean;
import utils.ConnectionUtil;

public class GetJob {
	// 配置文件
	private static Properties properties = new Properties();
	// 原始链接
	private static Connection con = null;
	private static Statement stmt = null;
	private static final String bgs = "(";
	private static final String eds = ")";
	private static final String sp = ",";
	private static final Integer maxSubLine = 100; // 最大单次提交sql行
	private static final String lip = "'";
	private static  String hbasekey = "`hbasekey_bk`";
	private static  String jobinfo="`jobinfo_bk`";
	private static  String dbConfigId = "";
	
	public static void main(String[] args) throws Exception {
		
		dbConfigId="1";
		String dbName="SH_SMCVDMS_ASC_OWASC";
		InputStream in = new FileInputStream("E:/dttest/config1.properties");
		properties.load(in);
		getSrcConnection(); // 初始化链接
	
		//===可循环体
		getJobList(getDbconfigList(),dbName);
		getKeyDisappearJob(dbName);
		hbasekey  = "`hbasekey`";
		jobinfo ="`jobinfo`";
		//getJobList(getDbconfigList(),dbName); //插入正式表
		//===可循环体结束
		
		closeSrcConnection();// 关闭源链接
	}
	
	
	private static void getKeyDisappearJob(String dbName) throws SQLException, IOException{
		StringBuffer sBuffer = new StringBuffer();
		ArrayList<String> spList = new ArrayList<String>();
		spList.add("job_name,key_new,key_old,job_id");
		ResultSet rs = stmt.executeQuery("select * from (SELECT distinct(a.job_name) as job_name ,a.key_tp as kp_n ,b.key_tp as kp_o,b.job_id FROM "
																+ "jobinfo_bk a left join jobinfo b on a.job_name = b.job_name "
																+ "where a.src_db ='"
																+ dbName
																+ "' ) d where d.kp_n != d.kp_o");
		while (rs.next()) {
			sBuffer
						.append(rs.getString(1)).append(sp)
						.append(rs.getString(2)).append(sp)
						.append(rs.getString(3)).append(sp)
						.append(rs.getString(4));
			spList.add(sBuffer.toString());
			sBuffer.setLength(0);
		}
		//System.out.println(spList.toString());
		if (spList.size()>1) {
			String fileName = properties.getProperty("rptpath")+dbName+".csv";
			//String fileName = "E:/dttest/"+dbName+".csv";
			FileWriter writer = new FileWriter(fileName);
			//BufferedWriter buffer = new BufferedWriter(writer); 
			for (int i = 0; i < spList.size(); i++) {
				writer.write(spList.get(i)+"\n");
			}
			writer.close();
			System.out.println("注意！"+ dbName + "库 hbasekey 发生变化 详细列表见 "+fileName);
		}else {
			System.out.println(dbName + "库 hbasekey 无变化");
		}
	}

	// 根据配置取得链接
	private static void getSrcConnection() throws Exception {
		con = ConnectionUtil.getConnent(properties.getProperty("url"), properties.getProperty("user"),
				properties.getProperty("password"));
		stmt = con.createStatement();
	}

	// 关闭静态的链接资源
	private static void closeSrcConnection() throws Exception {
		stmt.close();
		con.close();
	}

	// 读取dbConfig表
	private static ArrayList<ConnBean> getDbconfigList() throws SQLException {
		ResultSet rs = stmt.executeQuery("select db_url,user_name,password,driver_name from DBconf where id in( "
				+ dbConfigId + ")");
		ArrayList<ConnBean> connBeans = new ArrayList<ConnBean>();
		while (rs.next()) {
			ConnBean connBean = new ConnBean();
			connBean.setUrl(rs.getString(1));
			connBean.setUsname(rs.getString(2));
			connBean.setPassword(rs.getString(3));
			connBean.setClassName(rs.getString(4));
			//connBean.setDbName(rs.getString(5));
			connBeans.add(connBean);
		}
		rs.close();
		return connBeans;
	}

	// 根据链接生成JOB列表
	private static void getJobList(ArrayList<ConnBean> connBeans,String dbName) throws Exception {
		Connection cons = null;
		Statement stmts = null;
		for (ConnBean cb : connBeans) {
			 cons = ConnectionUtil.getConnentByClass(cb.getUrl(), cb.getUsname(), cb.getPassword(),
					cb.getClassName());
			 stmts = cons.createStatement();
			if (cb.getClassName().equals("com.mysql.jdbc.Driver")) {
				mySqlGetKey(stmts, dbName);
			}else {
				oracleGetKey(stmts, dbName);
			}
			cons.close();
			stmts.close();
		}
	}

	private static void mySqlGetKey(Statement stmts, String dbName) throws Exception {
		StringBuffer sBuffer = new StringBuffer();
		ArrayList<String> srcList = new ArrayList<String>();
		ResultSet rs = stmts.executeQuery("SELECT  t.TABLE_SCHEMA as owner_name,t.TABLE_NAME as table_name,"
				+ " t.CONSTRAINT_TYPE as constraint_name,c.COLUMN_NAME as rowkey,"
				+ " c.ORDINAL_POSITION as position  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t,"
				+ " INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS c WHERE t.TABLE_NAME = c.TABLE_NAME"
				+ " AND t.TABLE_SCHEMA = '" + dbName +  "' AND t.CONSTRAINT_TYPE = 'PRIMARY KEY'"
				+ " AND c.CONSTRAINT_NAME = 'PRIMARY'");
		while (rs.next()) {
			sBuffer
					.append(lip).append(rs.getString(1)).append(lip).append(sp)
					.append(lip).append(rs.getString(2)).append(lip).append(sp)
					.append(lip).append(rs.getString(3)).append(lip).append(sp)
					.append(lip).append(rs.getString(4)).append(lip).append(sp)
					.append(lip).append(rs.getString(5)).append(lip);
			srcList.add(sBuffer.toString());
			sBuffer.setLength(0);
		}
		rs.close();
		//插入hbasekey
		instertHbaseKey(srcList, dbName);
		//插入 job
		instertJob(getMySqlSrcJobSqlList(getExistJob(dbName ), stmts, dbConfigId,dbName), dbName);
	}

	//查出该表下已有的job
	private static HashMap<String, String> getExistJob(String dbName ) throws Exception{
		HashMap<String, String> hashMap = new HashMap<String, String>();
		ResultSet rs = stmt.executeQuery("SELECT job_name,job_id FROM "+jobinfo+" where src_db = '"+dbName+"'");
		while (rs.next()) {
			hashMap.put(rs.getString(1), rs.getString(2));
		}
		rs.close();
		return hashMap;
	}
	
	//取得原始 mySqljob sql List
	private static ArrayList<String> getMySqlSrcJobSqlList(HashMap<String, String> hashMap,Statement stmts,String dbconfigId,String dbName) throws Exception{
		ArrayList<String> rtList = new ArrayList<String>();
		ArrayList<String> oldJobSqlList =new ArrayList<String>();
		StringBuffer buffer = new StringBuffer();
		ResultSet rs =  stmts.executeQuery("SELECT concat(a.TABLE_SCHEMA,'.',a.TABLE_NAME),0,"+dbconfigId+",a.TABLE_SCHEMA,a.TABLE_NAME, a.TABLE_SCHEMA,a.TABLE_NAME,'_ALLCOLUMN_',CASE a.TABLE_ROWS when 0 then 1 else 0 end  FROM information_schema.`TABLES` a where a.TABLE_SCHEMA = '"
				+ dbName
				+ "'");
		while (rs.next()) {
				buffer
						.append(lip).append(rs.getString(1)).append(lip).append(sp)
						.append(lip).append(rs.getString(2)).append(lip).append(sp)
						.append(lip).append(rs.getString(3)).append(lip).append(sp)
						.append(lip).append(rs.getString(4)).append(lip).append(sp)
						.append(lip).append(rs.getString(5)).append(lip).append(sp)
						.append(lip).append(rs.getString(6)).append(lip).append(sp)
						.append(lip).append(rs.getString(7)).append(lip).append(sp)
						.append(lip).append(rs.getString(8)).append(lip).append(sp)
						.append(lip).append(rs.getString(9	)).append(lip);
			if (hashMap.get(rs.getString(1)) == null) {
				rtList.add(buffer.toString());
				buffer.setLength(0);
			}else {
				oldJobSqlList.add(lip+hashMap.get(rs.getString(1)) +lip+sp+buffer.toString());
				buffer.setLength(0);
			}
		}
		rs.close();
		updateOldJob(oldJobSqlList , dbName);
		return rtList;
	}
	
	//插入job 到MySql
	private static void instertJob(ArrayList<String> srcSqlList ,String dbName) throws Exception{
		final String baseSql = "INSERT INTO "+jobinfo+" (`job_id`,`job_name`,`enable`,`db_conf_id`,`src_db`,`src_table`,`des_db`,`des_table`,`key_tp`,`empty`) VALUES";
		final String baseFun = "getjobId("+dbConfigId+sp+lip +dbName+lip+eds+sp;
		StringBuffer bfto = new StringBuffer();
		int lin = 0;
		final int tonesize = srcSqlList.size() - 1;
		for (int i = 0; i <= tonesize; i++) {
			
			if (lin == 0) {
				bfto.append(baseSql);
			} 
			bfto.append(bgs).append(baseFun).append(srcSqlList.get(i)).append(eds);
			
			if (lin == maxSubLine || i == tonesize) {
				lin = 0;
				stmt.executeUpdate((bfto.toString()));
				System.out.println(bfto.toString());  //验证sql无问题后提交
				bfto.setLength(0);
				bfto.append(baseSql);
			} else {
				bfto.append(sp);
			}
			++lin;
		}
		//更新job的KEY
		String sql = "update "+jobinfo+" set key_tp ='KEY' where src_db = '"+dbName+"' and src_table in "
					+ "(select table_name from"
					+ hbasekey
					+ " where owner_name = '"+dbName+"')";
		stmt.executeUpdate(sql);
	}
	
	//插入hbaseKey到mySql
	private static void instertHbaseKey(ArrayList<String> srcList ,String dbName) throws Exception{
		final String baseString = "INSERT INTO "+hbasekey+" (`owner_name`,`table_name`,`constraint_name`,`rowkey`,`position`) VALUES ";
		stmt.executeUpdate("delete from "+hbasekey+" where  owner_name = '"+dbName+"'");
		// 组装sql
		StringBuffer bfto = new StringBuffer();
		int lin = 0;
		final int tonesize = srcList.size() - 1;
		for (int i = 0; i <= tonesize; i++) {
			if (lin == 0) {
				bfto.append(baseString);
			} 
			bfto.append(bgs).append(srcList.get(i)).append(eds);
			if (lin == maxSubLine || i == tonesize) {
				lin = 0;
				stmt.executeUpdate((bfto.toString()));
				bfto.setLength(0);
				bfto.append(baseString);
			} else {
				bfto.append(sp);
			}
			++lin;
		}
	}
	
	private static void oracleGetKey(Statement stmts,String dbName) throws Exception {
		StringBuffer sBuffer = new StringBuffer();
		ArrayList<String> srcList = new ArrayList<String>();
		ResultSet rs = stmts.executeQuery("Select * From (select a.owner as owner_name,a.table_name as table_name,'PK' as constraint_name,b.COLUMN_NAME as rowkey,1 as position from "
				+ "(SELECT * FROM SYS.ALL_constraints  where SYS.ALL_constraints.CONSTRAINT_TYPE='P'  and OWNER ='"
				+ dbName
				+ "' ) "
				+ "a join (SELECT * FROM sys.all_cons_columns )b on(a.constraint_name = b.constraint_name)"
				+ "group by a.owner,a.table_name,3,b.COLUMN_NAME "
				+ "union all SELECT i.TABLE_OWNER as owner_name,i.TABLE_NAME as table_name,i.INDEX_NAME as constraint_name,t.COLUMN_NAME as rowkey,t.COLUMN_POSITION as position FROM SYS.ALL_IND_COLUMNS t "
				+ "JOIN SYS.ALL_INDEXES i ON(t.index_name = i.index_name and t.table_name = i.table_name and t.TABLE_OWNER=i.TABLE_OWNER) where i.TABLE_OWNER ='"
				+ dbName
				+ "' AND i.UNIQUENESS = 'UNIQUE' AND not EXISTS"
				+ "(select 1 from SYS.ALL_constraints a where a.CONSTRAINT_TYPE='P' and  a.owner=i.TABLE_OWNER and a.table_name=i.TABLE_NAME) GROUP BY i.TABLE_OWNER,i.TABLE_NAME,i.INDEX_NAME,t.COLUMN_NAME,t.COLUMN_POSITION ORDER BY "
				+ "owner_name,table_name,position) Where table_name not like('BIN$'||'%')");
		while (rs.next()) {
			sBuffer
					.append(lip).append(rs.getString(1)).append(lip).append(sp)
					.append(lip).append(rs.getString(2)).append(lip).append(sp)
					.append(lip).append(rs.getString(3)).append(lip).append(sp)
					.append(lip).append(rs.getString(4)).append(lip).append(sp)
					.append(lip).append(rs.getString(5)).append(lip);
			srcList.add(sBuffer.toString());
			//System.out.println(sBuffer.toString());
			sBuffer.setLength(0);
		}
		rs.close();
		//插入hbasekey
		instertHbaseKey(srcList, dbName);
		
		//插入 job
		instertJob(getOracleSrcJobSqlList(getExistJob(dbName ), stmts, dbConfigId,dbName), dbName);
	}
	
	//取得原始 oracle job sql List
	private static ArrayList<String> getOracleSrcJobSqlList(HashMap<String, String> hashMap,Statement stmts,String dbconfigId,String dbName) throws Exception{
		ArrayList<String> rtList = new ArrayList<String>();
		ArrayList<String> oldJobSqlList = new ArrayList<String>();
		StringBuffer buffer = new StringBuffer();
		ResultSet rs =  stmts.executeQuery("SELECT a.TABLE_OWNER||'.'||a.TABLE_NAME,0,"
				+ dbconfigId
				+ ",a.TABLE_OWNER,a.TABLE_NAME, a.TABLE_OWNER,a.TABLE_NAME,'_ALLCOLUMN_',CASE a.NUM_ROWS when 0 then 1 else 0 end "
				+ "FROM (SELECT SYS.ALL_TABLES.TABLE_NAME AS TABLE_NAME,SYS.ALL_TABLES.OWNER AS TABLE_OWNER,SYS.ALL_TABLES.NUM_ROWS FROM SYS.ALL_TABLES where SYS.ALL_TABLES.OWNER = '"
				+ dbName
				+ "') a  "
				+ "LEFT JOIN (select c.owner as table_owner,c.table_name,3,'key' as key from (SELECT * FROM SYS.ALL_constraints  where SYS.ALL_constraints.CONSTRAINT_TYPE='P' and OWNER ='"
				+ dbName
				+ "') c  "
				+ "join (SELECT * FROM sys.all_cons_columns )d on(c.constraint_name = d.constraint_name)group by c.owner,c.table_name,3,d.COLUMN_NAME) b ON (a.TABLE_OWNER=b.TABLE_OWNER and a.TABLE_NAME=b.TABLE_NAME) Order by a.TABLE_NAME");
		while (rs.next()) {
			buffer
			.append(lip).append(rs.getString(1)).append(lip).append(sp)
			.append(lip).append(rs.getString(2)).append(lip).append(sp)
			.append(lip).append(rs.getString(3)).append(lip).append(sp)
			.append(lip).append(rs.getString(4)).append(lip).append(sp)
			.append(lip).append(rs.getString(5)).append(lip).append(sp)
			.append(lip).append(rs.getString(6)).append(lip).append(sp)
			.append(lip).append(rs.getString(7)).append(lip).append(sp)
			.append(lip).append(rs.getString(8)).append(lip).append(sp)
			.append(lip).append(rs.getString(9	)).append(lip);
			if (hashMap.get(rs.getString(1)) == null) {
				rtList.add(buffer.toString());
				buffer.setLength(0);
			}else {
				oldJobSqlList.add(lip+hashMap.get(rs.getString(1)) +lip+sp+buffer.toString());
				buffer.setLength(0);
			}
		}
		rs.close();
		updateOldJob(oldJobSqlList , dbName);
		return rtList;
	}
	
	//对旧job做更新操作 需要修改逻辑对不进行去主键操作
	private static void updateOldJob(ArrayList<String> srcSqlList , String dbName) throws SQLException{
		final String baseSql = "replace  INTO "+jobinfo+" (`job_id`,`job_name`,`enable`,`db_conf_id`,`src_db`,`src_table`,`des_db`,`des_table`,`key_tp`,`empty`) VALUES";
		StringBuffer bfto = new StringBuffer();
		int lin = 0;
		final int tonesize = srcSqlList.size() - 1;
		for (int i = 0; i <= tonesize; i++) {
			
			if (lin == 0) {
				bfto.append(baseSql);
			} 
			bfto.append(bgs).append(srcSqlList.get(i)).append(eds);
			
			if (lin == maxSubLine || i == tonesize) {
				lin = 0;
				stmt.executeUpdate((bfto.toString()));
				//System.out.println(bfto.toString());  //验证sql无问题后提交
				bfto.setLength(0);
				bfto.append(baseSql);
			} else {
				bfto.append(sp);
			}
			++lin;
		}
	}
	
}
