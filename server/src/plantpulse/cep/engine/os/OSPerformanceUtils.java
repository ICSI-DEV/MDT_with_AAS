package plantpulse.cep.engine.os;

import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import plantpulse.cep.engine.monitoring.event.OSPerformance;

/**
 * OSPerformanceUtils
 * 
 * @author lsb
 *
 */
public class OSPerformanceUtils {
	
	private static final Log log = LogFactory.getLog(OSPerformanceUtils.class);

	/**
	 * getOperatingSystemPerf
	 * 
	 * @return
	 */
	public static OSPerformance getOSPerformance() 
	{
		OSPerformance of = new OSPerformance();
		try {
			

			Sigar sigar = OSEnv.getInstance().getSigar();

			
			//CPU
			of.setUsed_cpu_percent(dx(sigar.getCpuPerc().getCombined() * 100));
			of.setFree_cpu_percent(dx(100 - of.getUsed_cpu_percent()));

			// 메모리
			of.setTotal_memory_size(sigar.getMem().getTotal());
			of.setHalf_memory_size(sigar.getMem().getTotal() / 2);
			of.setFree_memory_size(sigar.getMem().getFree());
			of.setUsed_memory_percent(dx(sigar.getMem().getUsedPercent()));
			of.setFree_memory_percent(dx(sigar.getMem().getFreePercent()));

			// DISK
			 int i = 0;
			 for (Path root : LinuxDiskPaths.getPaths()) {
		        	     try {
							FileStore store = Files.getFileStore(root);
							if (i == 0) {
								of.setPath_disk_name_1(root.toString());
								of.setFree_disk_size_1(store.getTotalSpace() - store.getUsableSpace());
								of.setTotal_disk_size_1(store.getTotalSpace());
							}
							if (i == 1) {
								of.setPath_disk_name_2(root.toString());
								of.setFree_disk_size_2(store.getTotalSpace() - store.getUsableSpace());
								of.setTotal_disk_size_2(store.getTotalSpace());
							}
							if (i == 2) {
								of.setPath_disk_name_3(root.toString());
								of.setFree_disk_size_3(store.getTotalSpace() - store.getUsableSpace());
								of.setTotal_disk_size_3(store.getTotalSpace());
							}
							if (i == 3) {
								of.setPath_disk_name_4(root.toString());
								of.setFree_disk_size_4(store.getTotalSpace() - store.getUsableSpace());
								of.setTotal_disk_size_4(store.getTotalSpace());
							}
							if (i == 4) {
								of.setPath_disk_name_5(root.toString());
								of.setFree_disk_size_5(store.getTotalSpace() - store.getUsableSpace());
								of.setTotal_disk_size_5(store.getTotalSpace());
							};
							i++;
							of.setTotal_disk_size(i);
		        		}catch(Exception ex) {
			        		//
			        	}
		      }
             
		} catch (Exception e1) {
			log.error("OS Performance getting error : " + e1.getMessage(), e1);
		}
		return of;

	};
	
	
	private static double calculateCPUAvg(Sigar sigar) {
	    int numCores = Runtime.getRuntime().availableProcessors();
	    double avg = 0;
	    try {
	        double[] cpuLoad = sigar.getLoadAverage();
	        for (int i = 0; i < numCores; i++) {
	        	avg =  ( cpuLoad[i] /= numCores);
	        }
	    } catch (SigarException ex) {
	        ex.printStackTrace();
	    }
	    return avg;
	}

	private static double dx(double d) {
		double per = Double.parseDouble(String.format("%.2f", d));
		if(Double.isNaN(per)) {
			per = 0.0d;
		}
		return per;
	}
}
