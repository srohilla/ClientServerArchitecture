package gash.router.database;

import java.sql.SQLException;

public class DatabaseService {
	
	protected DatabaseClient db;
	private String url = null;
	private String username = null;
	private String password = null;
	private String dbType = null;
	protected String ssl="false";

	private static DatabaseService instance = null;
	
	public static DatabaseService getInstance() {
		if (instance == null) {
			instance = new DatabaseService();			
		}
		return instance;
	}
	
	private DatabaseService() {
	}
	
	public DatabaseClient getDb() {
		try {
		if (db == null) {
			if (dbType.equalsIgnoreCase("cassandra")) {
				//db = new MyCassandraDB("127.0.0.1","db275");
			} else if (dbType.equalsIgnoreCase("postgresql")) {				
					db = new PostgreSQL(url, username, password, ssl);
				} 
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return db;
	}

	public String getUrl() {
		return url;
	}

	public void dbConfiguration(String type, String url, String username, String password) {
		this.dbType = type;
		this.url = url;
		this.username = username;
		this.password = password;
	}
}
