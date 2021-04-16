package plantpulse.cep.engine.monitoring.statement;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import com.espertech.esper.client.EventBean;

import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;

public class OSPerformanceStatement extends Statement {

	public OSPerformanceStatement(String serviceId, String name) {
		super(serviceId, name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.realdisplay.cep.engine.monitoring.EPLProvider#getEPL()
	 */
	@Override
	public String getEPL() {
		return "SELECT current_timestamp() as timestamp, * FROM plantpulse.cep.engine.monitoring.event.OSPerformance.win:time_batch(1 sec) output every 1 seconds";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.espertech.esper.client.UpdateListener#update(com.espertech.esper.
	 * client.EventBean[], com.espertech.esper.client.EventBean[])
	 */
	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {

		try {
			if (newEvents == null) {
				return;
			}
			EventBean event = newEvents[0];
			if (event != null) {
				Map<String, Object> data_map = new HashMap<String, Object>();

				data_map.put("timestamp", event.get("timestamp"));


			   //
				data_map.put("free_cpu_percent", event.get("free_cpu_percent"));
				data_map.put("used_cpu_percent", event.get("used_cpu_percent"));

				data_map.put("free_memory_percent", event.get("free_memory_percent"));
				data_map.put("used_memory_percent", event.get("used_memory_percent"));

				data_map.put("total_memory_size", event.get("total_memory_size"));
				data_map.put("half_memory_size", event.get("half_memory_size"));
				data_map.put("free_memory_size", event.get("free_memory_size"));
				data_map.put("used_memory_size", event.get("used_memory_size"));

	
				data_map.put("total_disk_size", event.get("total_disk_size"));

				data_map.put("path_disk_name_1", event.get("path_disk_name_1"));
				data_map.put("free_disk_size_1", event.get("free_disk_size_1"));
				data_map.put("total_disk_size_1", event.get("total_disk_size_1"));

				data_map.put("path_disk_name_2", event.get("path_disk_name_2"));
				data_map.put("free_disk_size_2", event.get("free_disk_size_2"));
				data_map.put("total_disk_size_2", event.get("total_disk_size_2"));

				data_map.put("path_disk_name_3", event.get("path_disk_name_3"));
				data_map.put("free_disk_size_3", event.get("free_disk_size_3"));
				data_map.put("total_disk_size_3", event.get("total_disk_size_3"));

				data_map.put("path_disk_name_4", event.get("path_disk_name_4"));
				data_map.put("free_disk_size_4", event.get("free_disk_size_4"));
				data_map.put("total_disk_size_4", event.get("total_disk_size_4"));

				data_map.put("path_disk_name_5", event.get("path_disk_name_5"));
				data_map.put("free_disk_size_5", event.get("free_disk_size_5"));
				data_map.put("total_disk_size_5", event.get("total_disk_size_5"));
				

				//
				JSONObject json = JSONObject.fromObject(data_map);
				PushClient client2 = ResultServiceManager.getPushService().getPushClient(MonitorConstants.PUSH_URL_SYSTEM_PERF + "/" + serviceId);
				client2.sendJSON(json);
				//
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

}
