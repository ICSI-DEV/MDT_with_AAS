package plantpulse.cep.service.client;

import org.springframework.stereotype.Service;

import plantpulse.cep.engine.storage.dao.DAOFactory;
import plantpulse.cep.engine.storage.insert.StorageInsertDAO;
import plantpulse.cep.engine.storage.select.StorageSelectDAO;

/**
 * StorageClient
 * 
 * @author lsb
 *
 */
@Service
public class StorageClient {

	private DAOFactory dao_factory = new DAOFactory();
	
	/**
	 * 스테이트먼트 빌드 (준비)
	 * @throws Exception
	 */
	public void buildPreparedStatement() throws Exception{
		dao_factory.buildPreparedStatement();
	}
	
	/**
	 * 조회를 위한 DAO 반환
	 * @return
	 * @throws Exception
	 */
	public StorageSelectDAO forSelect() throws Exception {
		return dao_factory.getSelectDAO();
	}
	
	/**
	 * 입력/수정/삭제를 위한 DAO 반환
	 * @return
	 * @throws Exception
	 */
	public StorageInsertDAO forInsert() throws Exception {
		return dao_factory.getInsertDAO();
	}

}
