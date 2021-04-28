package plantpulse.cep.domain;

import java.util.Date;

public class Dashboard {

	// 스텝 1
	private String dashboard_id;
	private String dashboard_title;
	private String dashboard_desc;
	private String dashboard_json;

	private boolean share;
	private String  share_security_id;
	
	private String insert_user_id;
	private Date insert_date;
	private Date last_update_date;

	public String getDashboard_id() {
		return dashboard_id;
	}

	public void setDashboard_id(String dashboard_id) {
		this.dashboard_id = dashboard_id;
	}

	public String getDashboard_title() {
		return dashboard_title;
	}

	public void setDashboard_title(String dashboard_title) {
		this.dashboard_title = dashboard_title;
	}

	public String getDashboard_desc() {
		return dashboard_desc;
	}

	public void setDashboard_desc(String dashboard_desc) {
		this.dashboard_desc = dashboard_desc;
	}

	public String getDashboard_json() {
		return dashboard_json;
	}

	public void setDashboard_json(String dashboard_json) {
		this.dashboard_json = dashboard_json;
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

	public boolean isShare() {
		return share;
	}

	public void setShare(boolean share) {
		this.share = share;
	}

	public String getShare_security_id() {
		return share_security_id;
	}

	public void setShare_security_id(String share_security_id) {
		this.share_security_id = share_security_id;
	}

	
}
