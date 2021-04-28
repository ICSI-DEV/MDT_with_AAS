package plantpulse.cep.engine.scheduling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.TriggerListener;

/**
 * JobTriggerListener
 * 
 * @author leesa
 *
 */
public class JobTriggerListener implements TriggerListener {
	
	private static final Log log = LogFactory.getLog(JobTriggerListener.class);

    private static final String TRIGGER_LISTENER_NAME = "PP_GLOBAL_TRIGGER_LISTENER";

    @Override
    public String getName() {
        return TRIGGER_LISTENER_NAME;
    }

    @Override
    public void triggerFired(Trigger trigger, JobExecutionContext context) {
        String triggerName = context.getJobDetail().getKey().toString();
        log.debug("Trigger : " + triggerName + " is fired");
    }

    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
        boolean veto = false;
        log.debug("Veto Job Excecution trigger: " + veto);
        return veto;
    }

    @Override
    public void triggerMisfired(Trigger trigger) {
    	log.debug(getName() + " trigger: " + trigger.getKey() + " misfired at " + trigger.getStartTime());
    }

    @Override
    public void triggerComplete(Trigger trigger, JobExecutionContext context, Trigger.CompletedExecutionInstruction triggerInstructionCode) {
    	log.debug(getName() + " trigger: " + trigger.getKey() + " completed at " + trigger.getStartTime());
    }
}