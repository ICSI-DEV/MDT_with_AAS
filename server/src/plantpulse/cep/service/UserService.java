package plantpulse.cep.service;

import java.util.List;
import java.util.Map;

import plantpulse.cep.dao.UserDAO;
import plantpulse.domain.User;

/**
 * UserService
 * @author leesa
 *
 */
public class UserService {

	private UserDAO dao = new UserDAO();

	public boolean checkLogin(String userId, String password) throws Exception {
		return dao.checkLogin(userId, password);
	}

	public List<User> getUserList() throws Exception {
		return dao.getUserList();
	}

	public User getUser(String user_id) throws Exception {
		return dao.getUser(user_id);
	}

	public List<User> getTagAlarmRecipient() throws Exception {
		return dao.getTagAlarmRecipient();
	}

	public void saveUser(User user) throws Exception {
		dao.saveUser(user);
	}

	public void updateUser(User user) throws Exception {
		dao.updateUser(user);
	}

	public void deleteUser(String user_id) throws Exception {
		dao.deleteUser(user_id);
	}

	public void updateSessionProperty(String login_id, String session_key, String session_value) throws Exception {
		dao.updateSessionProperty(login_id, session_key, session_value);
	}

	public String getSessionProperty(String login_id, String session_key) throws Exception {
		return dao.getSessionProperty(login_id, session_key);
	}
	public Map<String, String> getSessionPropertyMap(String login_id) throws Exception {
		return dao.getSessionPropertyMap(login_id);
	}
}
