package plantpulse.server.mvc;

public class Result {

	public static final String SUCCESS = "SUCCESS";
	public static final String WARNING = "WARNING";
	public static final String ERROR = "ERROR";

	private String status;
	private String message;

	public Result(String status, String message) {
		super();
		this.status = status;
		this.message = message;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
