package gash.router.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class PostgreSQL implements DatabaseClient {
	Connection conn = null;

	public PostgreSQL(String url, String username, String password, String ssl) throws SQLException {
		
		Properties props = new Properties();
		props.setProperty("user", username);
		props.setProperty("password", password);
		props.setProperty("ssl", ssl);
		conn = DriverManager.getConnection(url, props);
	}

	@Override
	public byte[] get(String key) {
		Statement stmt = null;
		byte[] image=null; 
		System.out.println(key);
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("Select image FROM testtable WHERE \"key\" LIKE '"+key+"'");
			
			while (rs.next()) {
				image=rs.getBytes(1);
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
		return image;		
	}

	@Override
	public List<Record> getNewEntries(long staleTimestamp) {
		Statement stmt = null;
		List<Record> list = new ArrayList<Record>();
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("Select key, image, timestamp FROM testtable where timestamp > " + staleTimestamp);
			
			while (rs.next()) {
				list.add(new Record(rs.getString(1), rs.getBytes(2), rs.getLong(3)));
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
		return list;		
	}


	@Override
	public List<Record> getAllEntries() {
		Statement stmt = null;
		List<Record> list = new ArrayList<Record>();
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("Select key, image, timestamp FROM testtable");
			
			while (rs.next()) {
				list.add(new Record(rs.getString(1), rs.getBytes(2), rs.getLong(3)));
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
		return list;		
	}

	
	@Override
	public long getCurrentTimeStamp() {
		Statement stmt = null;
		long timestamp = 0; 
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("Select max(timestamp) FROM testtable");
			
			while (rs.next()) {
				timestamp = rs.getLong(1);
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
		return timestamp;		
	}

	
	@Override
	public String post(byte[] image, long timestamp){
		String key = UUID.randomUUID().toString();
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement("INSERT INTO testtable VALUES ( ?, ?, ?)");
			ps.setString(1, key);
			ps.setBytes(2, image);
			ps.setLong(3, timestamp);
			ResultSet set = ps.executeQuery();
			
		} catch (SQLException e) {
		} finally {
			try {
				if (ps != null)
					ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return key;
	}

	
	@Override
	public void post(String key, byte[] image, long timestamp){
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement("INSERT INTO testtable VALUES ( ?, ?, ?)");
			ps.setString(1, key);
			ps.setBytes(2, image);
			ps.setLong(3, timestamp);
			ResultSet set = ps.executeQuery();
			
		} catch (SQLException e) {
		} finally {
			try {
				if (ps != null)
					ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void put(String key, byte[] image, long timestamp){		
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement("UPDATE testtable SET image= ? , timestamp = ?  WHERE key LIKE ?");
			ps.setBytes(1, image);
			ps.setLong(2, timestamp);
			ps.setString(3, key);
			ResultSet set = ps.executeQuery();
			
		} catch (SQLException e) {
		} finally {
			try {
				if (ps != null)
					ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	
	
	@Override
	public void putEntries(List<Record> list){		
			for (Record record : list) {
				put(record.getKey(), record.getImage(), record.getTimestamp());
			}
	}

	
	@Override
	public void delete(String key){
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			StringBuilder sql = new StringBuilder();
			sql.append("DELETE FROM testtable WHERE key LIKE '"+key+"';");			
			stmt.executeUpdate(sql.toString());
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// initiate new everytime
		}
	}

}
