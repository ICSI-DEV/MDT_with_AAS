package plantpulse.cep.engine.monitoring.jmx;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * JMXAgent
 * 
 * @author leesa
 *
 */
public class JMXAgent {
	
	private static final Log log = LogFactory.getLog(JMXAgent.class);
	
	
	   public static final String JMX_NAME = "plantpulse:name=MBean";
	
	   private MBeanServer mbs = null;
	   
		private static class JMXAgentHolder {
			static JMXAgent instance = new JMXAgent();
		}

		public static JMXAgent getInstance() {
			return JMXAgentHolder.instance;
		}

		public void start() {
		      mbs = ManagementFactory.getPlatformMBeanServer();
		      PlantPulse mbean = new PlantPulse();
		      ObjectName plantpulse = null;
		      try {
		          //Uniquely identify the MBeans and register them with the platform MBeanServer 
		    	  plantpulse = new ObjectName(JMX_NAME);
		          mbs.registerMBean(mbean, plantpulse);
		      } catch(Exception e) {
		         log.error(e.getMessage(), e);
		      };
		      log.info("PlantPulse MBean registed. [plantpulse:name=MBean]");
	   }
		
		public void stop() {
		      mbs = ManagementFactory.getPlatformMBeanServer();
		      ObjectName plantpulse = null;
		      try {
		    	  plantpulse = new ObjectName(JMX_NAME);
		          mbs.unregisterMBean(plantpulse);
		      } catch(Exception e) {
		         log.error(e.getMessage(), e);
		      };
		      log.info("PlantPulse MBean unregisted. [plantpulse:name=MBean]");
	   }

	   public PlantPulse getMBean() throws Exception {
			   PlantPulse plantPulseMBean =
		           (PlantPulse)MBeanServerInvocationHandler.newProxyInstance(
		                mbs, new ObjectName(JMX_NAME), PlantPulse.class, true);
		        return plantPulseMBean;
	   }
	 
}