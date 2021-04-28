package plantpulse.cep.engine.os;

import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.Sigar;

import plantpulse.cep.context.EngineContext;

/**
 * OSEnv
 * 
 * @author leesa
 *
 */
public class OSEnv {
	
	private static final Log log=LogFactory.getLog(OSEnv.class);

	private static class OSEnvHolder {
		static OSEnv instance=new OSEnv();
	}

	public static OSEnv getInstance() {
		return OSEnvHolder.instance;
	}

	private static final String SIGAR_PATH="/path/sigar/lib/";
	
	private Sigar sigar=null;
	
	public void configure() {
		try {
			 //
			 String root=(String) EngineContext.getInstance().getProps().get("_ROOT");
			 //
			 String base_java_library_path=System.getProperty("java.library.path");
	    	 System.setProperty("java.library.path", base_java_library_path + ":" + root + "/../../" + SIGAR_PATH);
	    	 //
	    	 log.info("OSEnv : java.library.path=" + System.getProperty("java.library.path"));
	    	 log.info("Sigar inited for OS Performance metrics collecting.");
	         //
	    	 sigar=new Sigar(); //
	    	 //
	    	 log.info("MACHINE HOST NAME=[[[ " + sigar.getFQDN() + " ]]]");
	    	 log.info(toString());
             
             
             log.info("OSEnv configured completed.");
		}catch(Exception ex) {
			log.error("OSEnv configure failed : " + ex.getMessage(), ex);
		}
	};
	
	public Sigar getSigar() throws Exception {
		if(sigar == null) {
			throw new Exception("Sigar is not inited.");
		}
		return sigar;
	};
	
	
	public String toString() {
	    String ret="OS : Name : " + System.getProperty("os.name") + ""
	           + ", Version : " + System.getProperty("os.version") + ""
	           + ", Architecture : " + System.getProperty("os.arch") + " | ";
	    try {
	    	
	        ret += "Total Memory :" + (sigar.getMem().getTotal() / 1024 / 1024 / 1024) + " GBytes" + "|";
	        int i=0;
	        for(CpuInfo cpuInfo: sigar.getCpuInfoList()) {
	            ret +=  " CPU :"
	                + ", Model="+ cpuInfo.getModel() + ""
	                + ", Vendor="+ cpuInfo.getVendor() + ""
	                + ", Num of of Core=" + cpuInfo.getTotalCores() + ""
	                + ", Mhz=" + cpuInfo.getMhz() + " | ";
	             if(i == 0) break;
	        };
	        
	        
	        NumberFormat nf=NumberFormat.getNumberInstance();
	        int disk_count = 0;
	        //for (Path root : FileSystems.getDefault().getRootDirectories()) {
	        for (Path root : LinuxDiskPaths.getPaths()) {
	        	try {
	        		
		        	FileStore store=Files.getFileStore(root);
		        	//System.out.println(store.name());;
		        	disk_count++;
		        	ret +=  (" HDD_" + disk_count + ": ");
		            ret += (", Total=" + nf.format(store.getTotalSpace() / 1024 / 1024 / 1024) + " GBytes"+ 
		                		", Used=" + nf.format((store.getTotalSpace() - store.getUsableSpace()) / 1024 / 1024 / 1024) + " GBytes"+ "," + 
		                		", Available=" + nf.format(store.getUsableSpace() / 1024 / 1024 / 1024) + " GBytes" + "");
		            ret += " : ";
	        	}catch(Exception ex) {
	        		//
	        	}
	        };
	        ret += (" HDD Total Size : " + disk_count + " ");
	    } catch (Exception e) {
	    	log.error("OSEnv info get string failed : " + e.getMessage(), e);
	    }
	    return ret;
	}

}
