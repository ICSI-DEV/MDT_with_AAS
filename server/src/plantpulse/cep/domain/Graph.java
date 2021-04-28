package plantpulse.cep.domain;

import java.util.Date;

public class Graph {

	//
	private Query query;

	// 스텝 1
	private String dashboard_id;

	private String graph_id;
	private String graph_title;
	private String graph_priority;
	private String graph_desc;
	private String graph_json;

	private String insert_user_id;
	private Date insert_date;
	private Date last_update_date;

	// 스텝 2
	private String epl;
	private String ws_url;

	// 스텝3
	private String graph_type;

	//
	private String tag_id;
	private String time_window;

	// 바 타입
	private String bar_xkey;
	private String[] bar_ykeys;
	private String[] bar_labels;
	private boolean bar_stacked;
	private int bar_display;

	// 라인타입
	private String line_xkey;
	private String[] line_ykeys;
	private String[] line_labels;
	private boolean line_stacked;
	private int line_display;
	private Double[] line_goals;
	private String line_ymin;
	private String line_ymax;

	// 라인타입
	private String area_xkey;
	private String[] area_ykeys;
	private String[] area_labels;
	private boolean area_stacked;
	private int area_display;
	private Double[] area_goals;
	private String area_ymin;
	private String area_ymax;

	// 게이지 타입
	private String gauge_min;
	private String gauge_max;
	private String gauge_title;
	private String gauge_label;
	private String gauge_valuekey;

	// 파이 타입
	private String[] pie_keys;
	private String[] pie_labels;

	// 테이블 타입
	private String[] table_keys;
	private String[] table_labels;
	private String[] table_widths;
	private int table_display_row;
	private String table_function;
	private boolean table_stacked;

	// 텍스트 타입
	private String text_key;
	private String text_label;
	private String text_unit;
	private String text_function;

	// 아이프레임 타입
	
	//
	private int history_count_limit;

	public String getGraph_id() {
		return graph_id;
	}

	public void setGraph_id(String graph_id) {
		this.graph_id = graph_id;
	}

	public String getGraph_title() {
		return graph_title;
	}

	public void setGraph_title(String graph_title) {
		this.graph_title = graph_title;
	}

	public String getGraph_desc() {
		return graph_desc;
	}

	public void setGraph_desc(String graph_desc) {
		this.graph_desc = graph_desc;
	}

	public String getGraph_json() {
		return graph_json;
	}

	public void setGraph_json(String graph_json) {
		this.graph_json = graph_json;
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

	public String getEpl() {
		return epl;
	}

	public void setEpl(String epl) {
		this.epl = epl;
	}

	public String getWs_url() {
		return ws_url;
	}

	public void setWs_url(String ws_url) {
		this.ws_url = ws_url;
	}

	public String getGraph_type() {
		return graph_type;
	}

	public void setGraph_type(String graph_type) {
		this.graph_type = graph_type;
	}

	public String getBar_xkey() {
		return bar_xkey;
	}

	public void setBar_xkey(String bar_xkey) {
		this.bar_xkey = bar_xkey;
	}

	public String[] getBar_ykeys() {
		return bar_ykeys;
	}

	public void setBar_ykeys(String[] bar_ykeys) {
		this.bar_ykeys = bar_ykeys;
	}

	public String[] getBar_labels() {
		return bar_labels;
	}

	public void setBar_labels(String[] bar_labels) {
		this.bar_labels = bar_labels;
	}

	public boolean isBar_stacked() {
		return bar_stacked;
	}

	public void setBar_stacked(boolean bar_stacked) {
		this.bar_stacked = bar_stacked;
	}

	public int getBar_display() {
		return bar_display;
	}

	public void setBar_display(int bar_display) {
		this.bar_display = bar_display;
	}

	public String getDashboard_id() {
		return dashboard_id;
	}

	public void setDashboard_id(String dashboard_id) {
		this.dashboard_id = dashboard_id;
	}

	public String getLine_xkey() {
		return line_xkey;
	}

	public void setLine_xkey(String line_xkey) {
		this.line_xkey = line_xkey;
	}

	public String[] getLine_ykeys() {
		return line_ykeys;
	}

	public void setLine_ykeys(String[] line_ykeys) {
		this.line_ykeys = line_ykeys;
	}

	public String[] getLine_labels() {
		return line_labels;
	}

	public void setLine_labels(String[] line_labels) {
		this.line_labels = line_labels;
	}

	public boolean isLine_stacked() {
		return line_stacked;
	}

	public void setLine_stacked(boolean line_stacked) {
		this.line_stacked = line_stacked;
	}

	public int getLine_display() {
		return line_display;
	}

	public void setLine_display(int line_display) {
		this.line_display = line_display;
	}

	public String getArea_xkey() {
		return area_xkey;
	}

	public void setArea_xkey(String area_xkey) {
		this.area_xkey = area_xkey;
	}

	public String[] getArea_ykeys() {
		return area_ykeys;
	}

	public void setArea_ykeys(String[] area_ykeys) {
		this.area_ykeys = area_ykeys;
	}

	public String[] getArea_labels() {
		return area_labels;
	}

	public void setArea_labels(String[] area_labels) {
		this.area_labels = area_labels;
	}

	public boolean isArea_stacked() {
		return area_stacked;
	}

	public void setArea_stacked(boolean area_stacked) {
		this.area_stacked = area_stacked;
	}

	public int getArea_display() {
		return area_display;
	}

	public void setArea_display(int area_display) {
		this.area_display = area_display;
	}

	public String getGauge_min() {
		return gauge_min;
	}

	public void setGauge_min(String gauge_min) {
		this.gauge_min = gauge_min;
	}

	public String getGauge_max() {
		return gauge_max;
	}

	public void setGauge_max(String gauge_max) {
		this.gauge_max = gauge_max;
	}

	public String getGauge_title() {
		return gauge_title;
	}

	public void setGauge_title(String gauge_title) {
		this.gauge_title = gauge_title;
	}

	public String getGauge_label() {
		return gauge_label;
	}

	public void setGauge_label(String gauge_label) {
		this.gauge_label = gauge_label;
	}

	public String getGauge_valuekey() {
		return gauge_valuekey;
	}

	public void setGauge_valuekey(String gauge_valuekey) {
		this.gauge_valuekey = gauge_valuekey;
	}

	public String getGraph_priority() {
		return graph_priority;
	}

	public void setGraph_priority(String graph_priority) {
		this.graph_priority = graph_priority;
	}

	public String[] getPie_keys() {
		return pie_keys;
	}

	public void setPie_keys(String[] pie_keys) {
		this.pie_keys = pie_keys;
	}

	public String[] getPie_labels() {
		return pie_labels;
	}

	public void setPie_labels(String[] pie_labels) {
		this.pie_labels = pie_labels;
	}

	public String[] getTable_keys() {
		return table_keys;
	}

	public void setTable_keys(String[] table_keys) {
		this.table_keys = table_keys;
	}

	public String[] getTable_labels() {
		return table_labels;
	}

	public void setTable_labels(String[] table_labels) {
		this.table_labels = table_labels;
	}

	public String[] getTable_widths() {
		return table_widths;
	}

	public void setTable_widths(String[] table_widths) {
		this.table_widths = table_widths;
	}

	public String getTable_function() {
		return table_function;
	}

	public void setTable_function(String table_function) {
		this.table_function = table_function;
	}

	public String getText_key() {
		return text_key;
	}

	public void setText_key(String text_key) {
		this.text_key = text_key;
	}

	public String getText_label() {
		return text_label;
	}

	public void setText_label(String text_label) {
		this.text_label = text_label;
	}

	public String getText_function() {
		return text_function;
	}

	public void setText_function(String text_function) {
		this.text_function = text_function;
	}

	public int getTable_display_row() {
		return table_display_row;
	}

	public void setTable_display_row(int table_display_row) {
		this.table_display_row = table_display_row;
	}

	public Double[] getLine_goals() {
		return line_goals;
	}

	public void setLine_goals(Double[] line_goals) {
		this.line_goals = line_goals;
	}

	public Double[] getArea_goals() {
		return area_goals;
	}

	public void setArea_goals(Double[] area_goals) {
		this.area_goals = area_goals;
	}

	public String getLine_ymin() {
		return line_ymin;
	}

	public void setLine_ymin(String line_ymin) {
		this.line_ymin = line_ymin;
	}

	public String getLine_ymax() {
		return line_ymax;
	}

	public void setLine_ymax(String line_ymax) {
		this.line_ymax = line_ymax;
	}

	public String getArea_ymin() {
		return area_ymin;
	}

	public void setArea_ymin(String area_ymin) {
		this.area_ymin = area_ymin;
	}

	public String getArea_ymax() {
		return area_ymax;
	}

	public void setArea_ymax(String area_ymax) {
		this.area_ymax = area_ymax;
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public String getText_unit() {
		return text_unit;
	}

	public void setText_unit(String text_unit) {
		this.text_unit = text_unit;
	}

	public boolean isTable_stacked() {
		return table_stacked;
	}

	public void setTable_stacked(boolean table_stacked) {
		this.table_stacked = table_stacked;
	}

	public String getTime_window() {
		return time_window;
	}

	public void setTime_window(String time_window) {
		this.time_window = time_window;
	}

	public String getTag_id() {
		return tag_id;
	}

	public void setTag_id(String tag_id) {
		this.tag_id = tag_id;
	}

	public int getHistory_count_limit() {
		return history_count_limit;
	}

	public void setHistory_count_limit(int history_count_limit) {
		this.history_count_limit = history_count_limit;
	}

}
