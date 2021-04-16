package plantpulse.server.mvc.util;

/**
 * EngineConstants
 * 
 * @author lenovo
 *
 */
public class Constants {

	//
	public static final String _SESSION_USER_ID = "_session_user_id";
	public static final String _SESSION_USER = "_session_user";

	// 쿼리
	public static final String _QUERY_DEFAULT_STRING = "SELECT timestamp, COUNT(*) AS tag_count FROM Point.win:time(60 sec) OUTPUT LAST EVERY 1 SEC";

	// 그래프
	public static final String DEFAULT_GRAPH_PRIORITY = "default";
	public static final String DEFAULT_GRAPH_TYPE = "LINE";
	public static final String DEFAULT_GRAPH_EQL = "SELECT * FROM Point.win:length(1)";

	// DRS Type
	public static final String TYPE_SITE = "site";
	public static final String TYPE_OPC = "opc";
	public static final String TYPE_ASSET = "asset";
	public static final String TYPE_TAG = "tag";

	// AGENT
	public static final String AGENT_URL_PATH = "/rest.properties";

}
