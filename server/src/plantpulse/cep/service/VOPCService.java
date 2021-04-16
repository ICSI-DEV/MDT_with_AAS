package plantpulse.cep.service;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.dao.OPCDAO;
import plantpulse.domain.OPC;

@Service
public class VOPCService {

	private static final Log log = LogFactory.getLog(VOPCService.class);

	@Autowired
	private OPCDAO opc_dao;

	public List<OPC> selectOpcList(String opc_type) throws Exception {
		try {
			return opc_dao.selectOpcList(opc_type);
		} catch (Exception e) {
			log.warn("Can not read OPC list : " + e.getMessage(), e);
			throw e;
		}
	}

	public OPC selectOpc(OPC opc) throws Exception {
		try {
			return opc_dao.selectOpcInfo(opc);
		} catch (Exception e) {
			log.warn("Select VOPC Error : " + e.getMessage(), e);
			throw e;
		}
	}

	public void insertVOPC(OPC opc) throws Exception {
		try {
			opc_dao.insertVituralOpc(opc);
		} catch (Exception e) {
			log.error("Insert VOPC Error : " + e.getMessage(), e);
			throw e;
		}
	}

	public void updateVOPC(OPC opc) throws Exception {
		try {
			opc_dao.updateVituralOpc(opc);
		} catch (Exception e) {
			log.error("Update VOPC Error : " + e.getMessage(), e);
			throw e;
		}
	}

	public void deleteVOPC(OPC opc) throws Exception {
		try {
			opc_dao.deleteVituralOpc(opc);
		} catch (Exception e) {
			log.error("Delete VOPC Error : " + e.getMessage(), e);
			throw e;
		}
	}

}
