package plantpulse.cep.domain;

import java.util.Date;

public class Control {
	
	private long control_seq;
	private Date control_date;
	private String control_user_id;
	
	private String site_id;
	private String site_name;
	private String opc_id;
	private String opc_name;
	private String asset_id;
	private String asset_name;
	private String tag_id;
	private String tag_name;
	
	private String write_value;
	private String result;
	private String message;
	
	private int error_code;
	private int error_description;
	
	private Date insert_date;

	public long getControl_seq() {
		return control_seq;
	}

	public void setControl_seq(long control_seq) {
		this.control_seq = control_seq;
	}

	public Date getControl_date() {
		return control_date;
	}

	public void setControl_date(Date control_date) {
		this.control_date = control_date;
	}

	public String getControl_user_id() {
		return control_user_id;
	}

	public void setControl_user_id(String control_user_id) {
		this.control_user_id = control_user_id;
	}

	public String getTag_id() {
		return tag_id;
	}

	public void setTag_id(String tag_id) {
		this.tag_id = tag_id;
	}

	public String getWrite_value() {
		return write_value;
	}

	public void setWrite_value(String write_value) {
		this.write_value = write_value;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public int getError_code() {
		return error_code;
	}

	public void setError_code(int error_code) {
		this.error_code = error_code;
	}

	public int getError_description() {
		return error_description;
	}

	public void setError_description(int error_description) {
		this.error_description = error_description;
	}

	public Date getInsert_date() {
		return insert_date;
	}

	public void setInsert_date(Date insert_date) {
		this.insert_date = insert_date;
	}

	public String getSite_id() {
		return site_id;
	}

	public void setSite_id(String site_id) {
		this.site_id = site_id;
	}

	public String getSite_name() {
		return site_name;
	}

	public void setSite_name(String site_name) {
		this.site_name = site_name;
	}

	public String getOpc_id() {
		return opc_id;
	}

	public void setOpc_id(String opc_id) {
		this.opc_id = opc_id;
	}

	public String getOpc_name() {
		return opc_name;
	}

	public void setOpc_name(String opc_name) {
		this.opc_name = opc_name;
	}

	public String getAsset_id() {
		return asset_id;
	}

	public void setAsset_id(String asset_id) {
		this.asset_id = asset_id;
	}

	public String getAsset_name() {
		return asset_name;
	}

	public void setAsset_name(String asset_name) {
		this.asset_name = asset_name;
	}

	public String getTag_name() {
		return tag_name;
	}

	public void setTag_name(String tag_name) {
		this.tag_name = tag_name;
	}
	

	
	
}
