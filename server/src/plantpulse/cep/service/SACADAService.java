package plantpulse.cep.service;

import java.util.List;

import plantpulse.cep.dao.SCADADAO;
import plantpulse.cep.domain.SCADA;

public class SACADAService  {

	private SCADADAO dao;

	public SACADAService() {
		this.dao = new SCADADAO();
	}

	public List<SCADA> getSCADAListByUserId(String insert_user_id) throws Exception {
		return dao.getScadaListByUserId(insert_user_id);
	}
	
	public List<SCADA> getScadaListBySecurityId(String security_id) throws Exception {
		return dao.getScadaListBySecurityId(security_id);
	}

	public SCADA getSCADAById(String scada_id) throws Exception {
		return dao.getScadaById(scada_id);
	}

	public void insertSCADA(SCADA scada) throws Exception {
		dao.insertSCADA(scada);
	}

	public void updateSCADA(SCADA scada) throws Exception {
		dao.updateSCADA(scada);
	}

	public void deleteSCADA(String scada_id) throws Exception {
		dao.deleteSCADA(scada_id);
	}

}
