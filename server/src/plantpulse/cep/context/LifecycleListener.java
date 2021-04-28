package plantpulse.cep.context;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.realdisplay.framework.util.StringUtils;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.config.ConfigurationManager;

/**
 * LifecycleListener
 * 
 * @author lenovo
 *
 */
public class LifecycleListener implements ServletContextListener {

	
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * javax.servlet.ServletContextListener#contextInitialized(javax.servlet
	 * .ServletContextEvent)
	 */
	@Override
	public void contextInitialized(ServletContextEvent servletContextEvent) {

		System.out.println("|===================================================================================|");
		System.out.println("|   PLANTPULSE SERVER START...                                                      |");
		System.out.println("|===================================================================================|");
		System.out.println("| - SMART FACTORY PLATFORM - IMPLEMENTING FACTORY OF THE FUTURE.                    |");
		System.out.println("| - REAL-TIME BIG DATA TECHNOLOGY-BASED PLATFORM FOR INDUSTRIAL SENSOR DATA.        |");
		System.out.println("| - SENSOR DATA MANAGEMENT IN WEB ENVIRONMENT                                       |");
		System.out.println("| - OPERATIONAL HISTORIAN                                                           |");
		System.out.println("| - COMPLEX EVENT PROCESSING(CEP) ENGINE FOR STREAMING ANALYSIS                     |");
		System.out.println("| - REAL-TIME ANALYTICS DASHBOARD                                                   |");
		System.out.println("| - REAL-TIME ALARM MANAGEMENT                                                      |");
		System.out.println("| - COPYRIGHT (C) 2012-2021 KOPENS, INC. ALL RESERVERD.                             |");
		System.out.println("|===================================================================================|");

		//
		ConfigurationManager.getInstance().init();
		
		//
		String root = StringUtils.cleanPath(servletContextEvent.getServletContext().getRealPath("/"));
		EngineContext.getInstance().getProps().put("_ROOT", root);
		CEPEngineManager.getInstance().start(root, false);

		//
		
		//
		System.out.println("===================================================================================");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see javax.servlet.ServletContextListener#contextDestroyed(javax.servlet.
	 * ServletContextEvent)
	 */
	@Override
	public void contextDestroyed(ServletContextEvent servletContextEvent) {
	
		//
		CEPEngineManager.getInstance().shutdown();
		
		System.out.println("===================================================================================");
	}

}
