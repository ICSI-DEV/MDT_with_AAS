package plantpulse.cep.engine.dds;

import org.json.JSONObject;

/**
 * 데이터 배포 서비스
 * 
 * <pre>
 * 플랫폼에서 발생된 데이터를 KAFKA를 통해 재배포한다.
 * 이는 외부 애플리케이션에서 메세지 컨슈머를 통해 플랫폼의 데이터를 실시간으로 사용할 수 있게 한다.
 * TODO 비동기로 변경
 * </pre>
 * @author lsb
 *
 */
public interface DataDistributionService {
	
	public  void sendTagPoint(String tag_id, JSONObject data);
	
	public  void sendTagAlarm(String tag_id, JSONObject data);

	public  void sendAssetData(String asset_id, JSONObject data);
	
	public  void sendAssetAlarm(String asset_id, JSONObject data);
	
	public  void sendAssetEvent(String asset_id, JSONObject data);
	
	public  void sendAssetContext(String asset_id, JSONObject data);
	
	public  void sendAssetAggregation(String asset_id, JSONObject data);
	
	
}
