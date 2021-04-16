package plantpulse.cep.domain;

import java.util.Map;

/**
 * BLOB
 * @author leesa
 *
 */
public class BLOB {

	private String tag_id;
	private long timestamp;
	private byte[] blob;
	private String file_path;
	private long file_size;
	private String mime_type;
	private Map<String, String> attribute;

	public BLOB(String tag_id, long timestamp, byte[] blob, String file_path, long file_size, String mime_type,
			Map<String, String> attribute) {
		super();
		this.tag_id = tag_id;
		this.timestamp = timestamp;
		this.blob = blob;
		this.file_path = file_path;
		this.file_size = file_size;
		this.mime_type = mime_type;
		this.attribute = attribute;
	};

	public String getTag_id() {
		return tag_id;
	}

	public void setTag_id(String tag_id) {
		this.tag_id = tag_id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public byte[] getBlob() {
		return blob;
	}

	public void setBlob(byte[] blob) {
		this.blob = blob;
	}

	public String getFile_path() {
		return file_path;
	}

	public void setFile_path(String file_path) {
		this.file_path = file_path;
	}

	public long getFile_size() {
		return file_size;
	}

	public void setFile_size(long file_size) {
		this.file_size = file_size;
	}

	public String getMime_type() {
		return mime_type;
	}

	public void setMime_type(String mime_type) {
		this.mime_type = mime_type;
	}

	public Map<String, String> getAttribute() {
		return attribute;
	}

	public void setAttribute(Map<String, String> attribute) {
		this.attribute = attribute;
	}

}
