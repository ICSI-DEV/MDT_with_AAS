package plantpulse.cep.engine.eventbus;

/**
 * 도매인 변경 관리자
 * 
 * @author lsb
 *
 */
public class DomainChangeManager {
	
	/**
	 * 이벤트 버스를 반환한다.
	 * 
	 * @return
	 */
	public static DomainChangeEventBus  getBus() {
		return new DefaultDomainChangeEventBus();
	}

}
