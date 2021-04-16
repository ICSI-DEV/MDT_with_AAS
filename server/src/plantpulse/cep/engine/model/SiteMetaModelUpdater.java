package plantpulse.cep.engine.model;


/**
 * SiteMetaModelUpdater
 * @author lsb
 *
 */
public interface SiteMetaModelUpdater {
	
	/**
	 * 사이트에 필요한 메타모델을 생성한다.
	 * 즉, HSQL의 테이블을 카산드라로 이관하여 생성한다.
	 * 
	 * @throws Exception
	 */
	public void update() throws Exception;

}
