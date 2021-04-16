package plantpulse.cep.engine.ntp;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;

/**
 * NTP DATE SYNC
 * @author leesa
 *
 */
public class NTPSync {
	
	private static final Log log = LogFactory.getLog(NTPSync.class);
	
	
	
	public static void main(String[] args) {
		NTPSync htp = new NTPSync();
		htp.sync("192.168.0.88");
	}
	

	
	public void sync(String ntp_ip) {
		NTPUDPClient client = new NTPUDPClient();
		try {
			
			 //
			 client.open();
			 InetAddress hostAddr = InetAddress.getByName(ntp_ip);
			 TimeInfo info = client.getTime(hostAddr);
			 info.computeDetails(); //compute offset/delay if not already done
			 
			 Long offsetValue = info.getOffset();
			 Long delayValue = info.getDelay();
			 String delay = (delayValue == null)   ? "N/A" : delayValue.toString();
			 String offset = (offsetValue == null) ? "N/A" : offsetValue.toString();
			 long time = info.getReturnTime();
			 //
			 Date server_date = new Date();
			 Date ntp_date = new Date(time);
			 SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			 log.info("NTP Date Synced : ntp_date=[" + f.format(ntp_date) + "], server_date=[" + f.format(server_date) + "], roundtrip_delay(ms)=[" + delay + "], clock_offset(ms)=[" + offset + "], timestamp=[" + time + "], ntp_server_ip=[" + ntp_ip + "]"); // offset in ms
			 //
		}catch(Exception ex) {
			log.error("NTP Date sync failed from [" + ntp_ip + "] error=[" + ex.getMessage() + "]", ex);
		}finally {
			 if(client.isOpen()) client.close();
		}
	}

}
