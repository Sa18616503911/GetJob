package bean;

public class HbaseKeyBean {

	private String ownerName="";
	private String tableName="";
	private String constraintName="";
	private String rowkey="";
	private String position="";
	
	public String getOwnerName() {
		return ownerName;
	}
	public void setOwnerName(String ownerName) {
		this.ownerName = ownerName;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getConstraintName() {
		return constraintName;
	}
	public void setConstraintName(String constraintName) {
		this.constraintName = constraintName;
	}
	public String getRowkey() {
		return rowkey;
	}
	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}
	public String getPosition() {
		return position;
	}
	public void setPosition(String position) {
		this.position = position;
	}
}
