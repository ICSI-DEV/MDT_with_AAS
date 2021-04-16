package plantpulse.cep.domain;

import java.util.Date;

public class SCADA {
	
	private String mode;
	// 스텝 1
	private String scada_id;
	private String scada_title;
	private String scada_desc;
	
	private String svg_content;
	private String tag_mapping_json;
	private String javascript_listener;
	private String html_listener;
	
	private boolean share;
	private String  share_security_id;

	private String insert_user_id;
	private Date   insert_date;
	private Date   last_update_date;
	
	public String getScada_id() {
		return scada_id;
	}
	public void setScada_id(String scada_id) {
		this.scada_id = scada_id;
	}
	public String getScada_title() {
		return scada_title;
	}
	public void setScada_title(String scada_title) {
		this.scada_title = scada_title;
	}
	public String getScada_desc() {
		return scada_desc;
	}
	public void setScada_desc(String scada_desc) {
		this.scada_desc = scada_desc;
	}
	public String getSvg_content() {
		return svg_content;
	}
	public void setSvg_content(String svg_content) {
		this.svg_content = svg_content;
	}
	public String getTag_mapping_json() {
		return tag_mapping_json;
	}
	public void setTag_mapping_json(String tag_mapping_json) {
		this.tag_mapping_json = tag_mapping_json;
	}
	public String getJavascript_listener() {
		return javascript_listener;
	}
	public void setJavascript_listener(String javascript_listener) {
		this.javascript_listener = javascript_listener;
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
	public String getHtml_listener() {
		return html_listener;
	}
	public void setHtml_listener(String html_listener) {
		this.html_listener = html_listener;
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
