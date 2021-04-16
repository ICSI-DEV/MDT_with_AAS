package plantpulse.cep.engine.eventbus;

/**
 * <B>도매인 변경 이벤트 버스</B>
 * 
 * <pre> 
 *  도매인(사이트,에셋,OPC,태그 등)의 정보가 변경될때 이 버스를 통해 오브젝트 ID를 전달한다.
 *  전달된 오브젝트 ID에 해당하는 도매인의 최종 변경본을 조회한 후, 카산드라 시계열 데이터 베이스와 동기화 작업을 진행하고 추가적인 커스텀 로직을 수행한다.
 * </pre>
 * 
 * @author lsb
 *
 */
public interface DomainChangeEventBus {

	public void onTagChanged(String tag_id)   throws Exception;
	public void onSiteChanged(String site_id)  throws Exception;
	public void onAssetChanged(String asset_id) throws Exception;
	public void onOPCChanged(String opc_id)   throws Exception;
	public void onAlarmConfigChanged(String alarm_config_id)   throws Exception;
	public void onUserChanged(String user_id)   throws Exception;
    public void onSecurityChanged(String user_id)   throws Exception;
    public void onTokenChanged(String token)   throws Exception;
    public void onMetadataChanged(String object_id)   throws Exception;

}
