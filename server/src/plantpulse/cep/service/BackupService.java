package plantpulse.cep.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.dao.BackupDAO;

@Service
public class BackupService {

	private static final Log log = LogFactory.getLog(BackupService.class);

	@Autowired
	private BackupDAO backup_dao;
	
	
	public void backup() throws Exception {
		backup_dao.backup();
	}

}
