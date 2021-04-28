package plantpulse.cep.domain;

import java.util.Date;

public class Alarm {

	private long alarm_seq;
	private Date alarm_date;
	private String priority;
	private String description;
	private Date insert_date;
	private String insert_user_id;

	private String is_read;
	private Date read_date;

	public String getIs_read() {
		return is_read;
	}

	public void setIs_read(String is_read) {
		this.is_read = is_read;
	}

	public Date getRead_date() {
		return read_date;
	}

	public void setRead_date(Date read_date) {
		this.read_date = read_date;
	}

	private String alarm_config_id;

	public long getAlarm_seq() {
		return alarm_seq;
	}

	public void setAlarm_seq(long alarm_seq) {
		this.alarm_seq = alarm_seq;
	}

	public Date getAlarm_date() {
		return alarm_date;
	}

	public void setAlarm_date(Date alarm_date) {
		this.alarm_date = alarm_date;
	}

	public String getPriority() {
		return priority;
	}

	public void setPriority(String priority) {
		this.priority = priority;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Date getInsert_date() {
		return insert_date;
	}

	public void setInsert_date(Date insert_date) {
		this.insert_date = insert_date;
	}

	public String getInsert_user_id() {
		return insert_user_id;
	}

	public void setInsert_user_id(String insert_user_id) {
		this.insert_user_id = insert_user_id;
	}

	public String getAlarm_config_id() {
		return alarm_config_id;
	}

	public void setAlarm_config_id(String alarm_config_id) {
		this.alarm_config_id = alarm_config_id;
	}

}
