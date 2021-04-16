package plantpulse.cep.engine.messaging.unmarshaller;

import java.util.Map;

public class UnmarshaledEvent {

	private String eventTypeName;
	private Map<String, Object> data;

	public UnmarshaledEvent(String eventTypeName, Map<String, Object> data) {
		super();
		this.eventTypeName = eventTypeName;
		this.data = data;
	}

	public String getEventTypeName() {
		return eventTypeName;
	}

	public void setEventTypeName(String eventTypeName) {
		this.eventTypeName = eventTypeName;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return "UnmarshaledEvent [eventTypeName=" + eventTypeName + ", data=" + data + "]";
	}

}
