package plantpulse.cep.domain;

import java.util.Date;

public class Application {

	private String application_id;
	private String application_title;
	private String application_desc;

	private String insert_user_id;
	private Date insert_date;
	private Date last_update_date;

	public String getApplication_id() {
		return application_id;
	}

	public void setApplication_id(String application_id) {
		this.application_id = application_id;
	}

	public String getApplication_title() {
		return application_title;
	}

	public void setApplication_title(String application_title) {
		this.application_title = application_title;
	}

	public String getApplication_desc() {
		return application_desc;
	}

	public void setApplication_desc(String application_desc) {
		this.application_desc = application_desc;
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
}
