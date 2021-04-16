package plantpulse.cep.version.update;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.version.VersionUpdate;

/**
 * V6_0_0_20180909 업데이트
 * 
 * - 카산드라 필드 추가
 * - HSQLDB 버전 히스토리 테이블 추가
 * - 
 * @author leesa
 *
 */
public class VersionUpdate_V6_0_0_20180909 implements VersionUpdate {

	private static final Log log = LogFactory.getLog(VersionUpdate_V6_0_0_20180909.class);
	
	@Override
	public long getVersion() {
		return 20180909L;
	}

	@Override
	public void upgrade() throws Exception {
		log.info("버전 " + getVersion() + " 업데이트 시작 ... ");
		//1.HSQLDB 업그레이드
		
		//2.카산드라 스키마 업그레이드
		log.info("버전 " + getVersion() + " 업데이트 완료  ... ");
	}

}
