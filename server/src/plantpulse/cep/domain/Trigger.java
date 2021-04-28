package plantpulse.cep.domain;

import java.util.Date;

public class Trigger {

	private String trigger_id;
	private String trigger_name;
	private String trigger_desc;

	private String epl;
	private String ws_url;
	private String status;
	private String mode;

	private boolean use_mq;
	private String mq_protocol;
	private String mq_destination;

	private boolean use_storage;
	private String storage_table;
	private String storage_table_primary_keys;
	private String storage_table_clustering_order_by_keys;
	private String storage_table_idx_keys;

	private TriggerAttribute[] trigger_attributes;

	private String insert_user_id;
	private Date insert_date;
	private Date last_update_date;

	public String getTrigger_id() {
		return trigger_id;
	}

	public void setTrigger_id(String trigger_id) {
		this.trigger_id = trigger_id;
	}

	public String getTrigger_name() {
		return trigger_name;
	}

	public void setTrigger_name(String trigger_name) {
		this.trigger_name = trigger_name;
	}

	public String getTrigger_desc() {
		return trigger_desc;
	}

	public void setTrigger_desc(String trigger_desc) {
		this.trigger_desc = trigger_desc;
	}

	public String getEpl() {
		return epl;
	}

	public void setEpl(String epl) {
		this.epl = epl;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public boolean isUse_mq() {
		return use_mq;
	}

	public void setUse_mq(boolean use_mq) {
		this.use_mq = use_mq;
	}

	public String getMq_destination() {
		return mq_destination;
	}

	public void setMq_destination(String mq_destination) {
		this.mq_destination = mq_destination;
	}

	public boolean isUse_storage() {
		return use_storage;
	}

	public void setUse_storage(boolean use_storage) {
		this.use_storage = use_storage;
	}

	public String getStorage_table() {
		return storage_table;
	}

	public void setStorage_table(String storage_table) {
		this.storage_table = storage_table;
	}

	public TriggerAttribute[] getTrigger_attributes() {
		return trigger_attributes;
	}

	public void setTrigger_attributes(TriggerAttribute[] trigger_attributes) {
		this.trigger_attributes = trigger_attributes;
	}

	public String getInsert_user_id() {
		return insert_user_id;
	}

	public void setInsert_user_id(String insert_user_id) {
		this.insert_user_id = insert_user_id;
	}

	public Date getInsert_date() {
		return insert_date;
	}

	public void setInsert_date(Date insert_date) {
		this.insert_date = insert_date;
	}

	public Date getLast_update_date() {
		return last_update_date;
	}

	public void setLast_update_date(Date last_update_date) {
		this.last_update_date = last_update_date;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public String getWs_url() {
		return ws_url;
	}

	public void setWs_url(String ws_url) {
		this.ws_url = ws_url;
	}

	public String getMq_protocol() {
		return mq_protocol;
	}

	public void setMq_protocol(String mq_protocol) {
		this.mq_protocol = mq_protocol;
	}

	public String getStorage_table_primary_keys() {
		return storage_table_primary_keys;
	}

	public void setStorage_table_primary_keys(String storage_table_primary_keys) {
		this.storage_table_primary_keys = storage_table_primary_keys;
	}

	public String getStorage_table_clustering_order_by_keys() {
		return storage_table_clustering_order_by_keys;
	}

	public void setStorage_table_clustering_order_by_keys(String storage_table_clustering_order_by_keys) {
		this.storage_table_clustering_order_by_keys = storage_table_clustering_order_by_keys;
	}

	public String getStorage_table_idx_keys() {
		return storage_table_idx_keys;
	}

	public void setStorage_table_idx_keys(String storage_table_idx_keys) {
		this.storage_table_idx_keys = storage_table_idx_keys;
	}
       
}
