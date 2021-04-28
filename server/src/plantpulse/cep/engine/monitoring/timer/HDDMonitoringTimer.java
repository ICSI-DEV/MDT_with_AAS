package plantpulse.cep.engine.monitoring.timer;

import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.os.LinuxDiskPaths;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.engine.utils.DoubleUtils;

/**
 * HDDMonitoringTimer
 * 
 * @author leesa
 *
 */
public class HDDMonitoringTimer  implements MonitoringTimer  {

	private static final Log log = LogFactory.getLog(HDDMonitoringTimer.class);

	private static final int TIMER_PERIOD = (1000 * 1 * 60 * 60); //1시간마다 한번씩 실행
	
	private static final double WARN_PERCENT  = 80.0d; //
	
	private static final double ERROR_PERCENT = 90.0d; //

	private ScheduledFuture<?> task;
	
	public void start() {
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				//
				try {
					 //
					 log.info("-- SYSTEM HDD CHECK ---- ");
					 for (Path root : LinuxDiskPaths.getPaths()) {
						 try {
							FileStore store = Files.getFileStore(root);
							String path = root.toString();
							long total = store.getTotalSpace();
						    long used  = store.getTotalSpace() - store.getUsableSpace();
							long free  = store.getUsableSpace();
		                    double used_percent = DoubleUtils.toFixed(((Double.parseDouble(used+"") / Double.parseDouble(total+"")) * 100.0), 2);
		                   // System.out.println(used_percent);
		                    String hdd_text = String.format("경로 =[%s] 현재 사용률=[%s], 전체 용량: %s, 사용량: %s, 남은량: %s", 
		                    		path, 
		                    		used_percent + "%", 
		                    		FileUtils.byteCountToDisplaySize(total), 
		                    		FileUtils.byteCountToDisplaySize(used),
		                    		FileUtils.byteCountToDisplaySize(free));
		                    
		                    log.info(hdd_text);
		                    
		                    if((used_percent) >= ERROR_PERCENT ) {
		                    	EngineLogger.error("HDD 용량 초과 에러 : "  + hdd_text);
		                    }else if((used_percent) >=  WARN_PERCENT) {
		                    	 EngineLogger.warn("HDD 용량 초과 경고 : "  + hdd_text);
		                    }
						 }catch(java.nio.file.NoSuchFileException ex) {
							//SKIP 
						 }
		        	   };
					//
				} catch (Exception e) {
					log.error("HDD Monitoring error : " + e.getMessage(), e);
				}
			}

		}, 1000 * 60 * 1, TIMER_PERIOD, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		task.cancel(true);
	}

	
}
