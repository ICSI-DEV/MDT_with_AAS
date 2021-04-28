package plantpulse.cep.domain;


public class AssetStatement {
	
	public static final String EVENT_TYPE = "EVENT";
	public static final String CONTEXT_TYPE = "CONTEXT";
	public static final String AGGREGATION_TYPE = "AGGREGATION";
	
	private String mode;
	private String asset_id;
	private String statement_name;
	private String type;
	private String eql;
	private String description;
	private String insert_date;
	private String last_update_date;
	
	
	public String getMode() {
		return mode;
	}
	public void setMode(String mode) {
		this.mode = mode;
	}
	public String getAsset_id() {
		return asset_id;
	}
	public void setAsset_id(String asset_id) {
		this.asset_id = asset_id;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getEql() {
		return eql;
	}
	public void setEql(String eql) {
		this.eql = eql;
	}
	public String getStatement_name() {
		return statement_name;
	}
	public void setStatement_name(String statement_name) {
		this.statement_name = statement_name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getInsert_date() {
		return insert_date;
	}
	public void setInsert_date(String insert_date) {
		this.insert_date = insert_date;
	}
	public String getLast_update_date() {
		return last_update_date;
	}
	public void setLast_update_date(String last_update_date) {
		this.last_update_date = last_update_date;
	}
	@Override
	public String toString() {
		return "AssetStatement [asset_id=" + asset_id + ", statement_name=" + statement_name + ", type=" + type + ", eql=" + eql + ", description=" + description + ", insert_date=" + insert_date
				+ ", last_update_date=" + last_update_date + "]";
	}
	
	
	

}
