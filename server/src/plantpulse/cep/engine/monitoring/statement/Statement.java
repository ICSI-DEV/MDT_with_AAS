package plantpulse.cep.engine.monitoring.statement;

import com.espertech.esper.client.UpdateListener;

public abstract class Statement implements UpdateListener {

	protected String serviceId;
	protected String name;

	public Statement(String serviceId, String name) {
		this.serviceId = serviceId;
		this.name = name;
	}

	public String getServiceId() {
		return serviceId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}

	abstract public String getEPL();

}
