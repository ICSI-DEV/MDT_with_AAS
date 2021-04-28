package plantpulse.cep.domain;

import java.util.Arrays;
import java.util.Date;

public class Event {

	private String event_name;
	private String event_desc;

	private EventAttribute[] event_attributes;

	private String insert_user_id;
	private Date insert_date;
	private Date last_update_date;

	private String mode;
	private String status;

	public String getEvent_name() {
		return event_name;
	}

	public void setEvent_name(String event_name) {
		this.event_name = event_name;
	}

	public String getEvent_desc() {
		return event_desc;
	}

	public void setEvent_desc(String event_desc) {
		this.event_desc = event_desc;
	}

	public EventAttribute[] getEvent_attributes() {
		return event_attributes;
	}

	public void setEvent_attributes(EventAttribute[] event_attributes) {
		this.event_attributes = event_attributes;
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

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	@Override
	public String toString() {
		return "Event [event_name=" + event_name + ", event_desc=" + event_desc + ", event_attributes=" + Arrays.toString(event_attributes) + ", insert_user_id=" + insert_user_id + ", insert_date="
				+ insert_date + ", last_update_date=" + last_update_date + ", mode=" + mode + ", status=" + status + "]";
	}

}
