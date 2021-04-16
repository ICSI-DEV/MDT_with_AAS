package plantpulse.cep.engine.messaging.jaas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.jaas.UserPrincipal;

import plantpulse.cep.dao.UserDAO;
import plantpulse.cep.engine.config.ConfigurationManager;

/**
 * ApolloLoginModule
 * 
 * @author lenovo
 *
 */
public class ApolloLoginModule implements LoginModule {

	private CallbackHandler handler;
	private Subject subject;
	private UserPrincipal userPrincipal;
	private GroupPrincipal rolePrincipal;
	private String login;
	private List<String> userGroups;

	@Override
	public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {

		handler = callbackHandler;
		this.subject = subject;
		//System.out.println("Apollo LoginModule initiailized.");
	}

	@Override
	public boolean login() throws LoginException {

		Callback[] callbacks = new Callback[2];
		callbacks[0] = new NameCallback("login");
		callbacks[1] = new PasswordCallback("password", true);

		try {

			// Properties properties =
			// PropertiesUtils.read(MessageKeys.MQ_CONFIG_PATH);

			handler.handle(callbacks);

			// System.out.println("Apollo login started...22");

			String name = ((NameCallback) callbacks[0]).getName();
			String password = String.valueOf(((PasswordCallback) callbacks[1]).getPassword());

			// Here we validate the credentials against some
			// authentication/authorization provider.
			// It can be a Database, an external LDAP,
			// a Web Service, etc.
			// For this tutorial we are just checking if
			// user is "user123" and password is "pass123"

			if (

			(name != null && password != null && name.equals(ConfigurationManager.getInstance().getServer_configuration().getMq_user())
					&& password.equals(ConfigurationManager.getInstance().getServer_configuration().getMq_password()) // 프로퍼티상의
																														// 관리자
																														// 아이디/패스워드
																														// 확인
			) || (name != null && password != null && (new UserDAO()).checkLogin(name, password) // DB상의
																									// 관리자
																									// 아이디/패스워드
																									// 확인
			)

			) {

				// We store the username and roles
				// fetched from the credentials provider
				// to be used later in commit() method.
				// For this tutorial we hard coded the
				// "admin" role

				login = name;
				userGroups = new ArrayList<String>();

				/*
				 * admin : use of the administrative web interface monitor :
				 * read only use of the administrative web interface config :
				 * use of the administrative web interface to access and change
				 * the broker configuration. connect : allows connections to the
				 * connector or virtual host create : allows creation destroy :
				 * allows destruction send : allows the user to send to the
				 * destination receive : allows the user to send to do
				 * non-destructive reads from the destination consume : allows
				 * the user to do destructive reads against a destination : All
				 * ac
				 */


				userGroups.add("read");
				userGroups.add("write");
				userGroups.add("admin");
				userGroups.add("monitor");
				userGroups.add("config");
				userGroups.add("connect");
				userGroups.add("create");
				userGroups.add("destroy");
				userGroups.add("send");
				userGroups.add("receive");
				userGroups.add("consume");
				userGroups.add("admins");
				userGroups.add("users");

				return true;
			}

			// If credentials are NOT OK we throw a LoginException
			throw new LoginException("Authentication failed");

		} catch (IOException e) {
			throw new LoginException(e.getMessage());
		} catch (UnsupportedCallbackException e) {
			throw new LoginException(e.getMessage());
		}

	}

	@Override
	public boolean commit() throws LoginException {

		userPrincipal = new UserPrincipal(login);
		subject.getPrincipals().add(userPrincipal);

		if (userGroups != null && userGroups.size() > 0) {
			for (String groupName : userGroups) {
				rolePrincipal = new GroupPrincipal(groupName);
				subject.getPrincipals().add(rolePrincipal);
			}
		}
		// System.out.println("Apollo login started...444" );
		return true;
	}

	@Override
	public boolean abort() throws LoginException {
		return true;
	}

	@Override
	public boolean logout() throws LoginException {
		subject.getPrincipals().remove(userPrincipal);
		subject.getPrincipals().remove(rolePrincipal);
		return true;
	}

}