package bean;

public class ConnBean {
	private String url = "";
	private String password = "";
	private String usname = "";
	private String className = "";
	private String dbName = "";

	public String getUrl() {
		return url;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUsname() {
		return usname;
	}

	public void setUsname(String usname) {
		this.usname = usname;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

}
