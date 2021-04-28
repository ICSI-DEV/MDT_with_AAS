package plantpulse.cep.domain;

import java.util.Date;

public class Query {

	private String epl;
	private String id;
	private String url;
	private String error;
	private int expire_time;
	private int display_count;
	private String result_format;
	private boolean chart_fixed;

	private boolean show_line_chart;
	private String line_xkey;
	private String line_ykeys;

	private Long query_history_seq;
	private String insert_user_id;
	private Date insert_date;

	public String getEpl() {
		return epl;
	}

	public void setEpl(String epl) {
		this.epl = epl;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public int getExpire_time() {
		return expire_time;
	}

	public void setExpire_time(int expire_time) {
		this.expire_time = expire_time;
	}

	public int getDisplay_count() {
		return display_count;
	}

	public void setDisplay_count(int display_count) {
		this.display_count = display_count;
	}

	public String getResult_format() {
		return result_format;
	}

	public void setResult_format(String result_format) {
		this.result_format = result_format;
	}

	public boolean isChart_fixed() {
		return chart_fixed;
	}

	public void setChart_fixed(boolean chart_fixed) {
		this.chart_fixed = chart_fixed;
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

	public Long getQuery_history_seq() {
		return query_history_seq;
	}

	public void setQuery_history_seq(Long query_history_seq) {
		this.query_history_seq = query_history_seq;
	}

	public boolean isShow_line_chart() {
		return show_line_chart;
	}

	public void setShow_line_chart(boolean show_line_chart) {
		this.show_line_chart = show_line_chart;
	}

	public String getLine_xkey() {
		return line_xkey;
	}

	public void setLine_xkey(String line_xkey) {
		this.line_xkey = line_xkey;
	}

	public String getLine_ykeys() {
		return line_ykeys;
	}

	public void setLine_ykeys(String line_ykeys) {
		this.line_ykeys = line_ykeys;
	}

}
