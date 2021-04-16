package plantpulse.cep.service;

import java.util.List;

import plantpulse.cep.dao.ServerDAO;
import plantpulse.cep.domain.Server;

public class ServerService {

	private ServerDAO dao = new ServerDAO();

	public List<Server> getServerList() throws Exception {
		return dao.getServerList();
	}

	public Server getServer(String server_id) throws Exception {
		return dao.getServer(server_id);
	}

	public void saveServer(Server server) throws Exception {
		dao.saveServer(server);
	}

	public void updateServer(Server server) throws Exception {
		dao.updateServer(server);
	}

	public void deleteServer(String server_id) throws Exception {
		dao.deleteServer(server_id);
	}
}
