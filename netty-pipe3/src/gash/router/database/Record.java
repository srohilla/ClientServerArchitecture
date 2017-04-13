package gash.router.database;

public class Record {
	private String key = null;
	private byte[] image = null;
	private long timestamp = 0;
	
	public Record(String key, byte[] image, long timestamp) {
		this.key = key;
		this.image = image;
		this.timestamp = timestamp;
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public byte[] getImage() {
		return image;
	}
	public void setImage(byte[] image) {
		this.image = image;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
			
}
