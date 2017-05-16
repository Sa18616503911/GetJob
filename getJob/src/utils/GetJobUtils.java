package utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import bean.ConnBean;

public class GetJobUtils {
	private static String hbasekey = "";
	private static String jobinfo = "";
	private static final String bgs = "(";
	private static final String eds = ")";
	private static final String sp = ",";
	private static final Integer maxSubLine = 100; // 最大单次提交sql行
	private static final String lip = "'";
	/**
	 * 默认构造方法 默认 hbasekey表为 hbasekey jobinfo表为 jobinfo
	 */
	public GetJobUtils() {
		hbasekey = "`hbasekey`";
		jobinfo = "`jobinfo`";
	}

	/**
	 * 指定 hbasekey & jobinfo表的表名
	 * @param hbasekey hbasekey表
	 * @param jobinfo jobinfo表
	 */
	public GetJobUtils(String hbasekey, String jobinfo) {
		GetJobUtils.hbasekey = hbasekey;
		GetJobUtils.jobinfo = jobinfo;
	}
	
	public  ArrayList<ConnBean> getDbconfigList(Statement stmt,String dbConfigId) throws SQLException {
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
	
	//查出该表下已有的job
	private  HashMap<String, String> getExistJob(String dbName,Statement stmt ) throws Exception{
		HashMap<String, String> hashMap = new HashMap<String, String>();
		ResultSet rs = stmt.executeQuery("SELECT job_name,job_id FROM "+jobinfo+" where src_db = '"+dbName+"'");
		while (rs.next()) {
			hashMap.put(rs.getString(1), rs.getString(2));
		}
		rs.close();
		return hashMap;
	}
	
	
	
	/**
	 * 插入Mysql的JOB 自动检测
	 * @param stmts job的 stmts
	 * @param dbName 
	 * @param stmtssrc 配置库的stmts
	 * @throws Exception
	 */
	public  void mySqlGetKey(Statement stmts, String dbName,Statement stmtssrc,String dbConfigId) throws Exception {
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
		this.instertHbaseKey(srcList, dbName,stmtssrc);
		//插入 job
		this.instertJob(getMySqlSrcJobSqlList(getExistJob(dbName ,stmtssrc), stmts, dbConfigId,dbName,stmtssrc), dbName,dbConfigId,stmtssrc);
	}
	
	//插入job 到MySql
		private  void instertJob(ArrayList<String> srcSqlList ,String dbName,String dbConfigId,Statement stmt) throws Exception{
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
		private  void instertHbaseKey(ArrayList<String> srcList ,String dbName,Statement stmt) throws Exception{
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
	
		//取得原始 mySqljob sql List
		private  ArrayList<String> getMySqlSrcJobSqlList(HashMap<String, String> hashMap,Statement stmts,String dbconfigId,String dbName,Statement stmtsrc) throws Exception{
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
			updateOldJob(oldJobSqlList , dbName,stmtsrc);
			return rtList;
		}

		private  void updateOldJob(ArrayList<String> srcSqlList , String dbName ,Statement stmt) throws SQLException{
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
		
		public  void oracleGetKey(Statement stmts,String dbName,Statement stmtsrc,String dbConfigId) throws Exception {
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
			this.instertHbaseKey(srcList, dbName,stmtsrc);
			
			//插入 job
			instertJob(getOracleSrcJobSqlList(getExistJob(dbName,stmtsrc), stmts, dbConfigId,dbName,stmtsrc), dbName,dbConfigId,stmtsrc);
		}
		
		private  ArrayList<String> getOracleSrcJobSqlList(HashMap<String, String> hashMap,Statement stmts,String dbconfigId,String dbName,Statement stmtsrc) throws Exception{
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
			updateOldJob(oldJobSqlList , dbName,stmtsrc);
			return rtList;
		}
}
