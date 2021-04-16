package plantpulse.cep.domain;

public class MessagingServer extends Server {
	private String protocol;
	private String topic;
	private String userName;
	private String password;

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public String toString() {
		return "MessagingServer [protocol=" + protocol + ", topic=" + topic + ", userName=" + userName + ", password=" + password + "]";
	}
}
