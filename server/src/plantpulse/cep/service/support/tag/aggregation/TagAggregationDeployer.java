package plantpulse.cep.service.support.tag.aggregation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EPStatement;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.domain.Tag;

/**
 * TagAggregationDeployer
 * 
 * @author leesa
 *
 */
public class TagAggregationDeployer  {

	private static final Log log = LogFactory.getLog(TagAggregationDeployer.class);

	/**
	 * 
	 * @param tag
	 * @param alarm_config
	 * @throws Exception
	 */
	public void deploy(Tag tag) throws Exception {

		try {
			    //
				if(tag.getUse_aggregation() != null && tag.getUse_aggregation().equals("Y")) {
					if(tag.getAggregation_1_minutes() != null && tag.getAggregation_1_minutes().equals("Y")){
						TagPointAggreationStatement pt_stmt = new TagPointAggreationStatement(tag, "1_MINUTES");
						EPStatement statement = CEPEngineManager.getInstance().getProvider().getEPAdministrator().createEPL(pt_stmt.getEPL(), pt_stmt.getName());
						statement.addListener(pt_stmt);
						statement.start();
						log.info("Aggregation tag deploy completed for tag_id=[" + tag.getTag_id() + "], stmt_name=[" + pt_stmt.getName() + "]");
					};
					if(tag.getAggregation_5_minutes() != null && tag.getAggregation_5_minutes().equals("Y")){
						TagPointAggreationStatement pt_stmt = new TagPointAggreationStatement(tag, "5_MINUTES");
						EPStatement statement = CEPEngineManager.getInstance().getProvider().getEPAdministrator().createEPL(pt_stmt.getEPL(), pt_stmt.getName());
						statement.addListener(pt_stmt);
						statement.start();
						log.info("Aggregation tag deploy completed for tag_id=[" + tag.getTag_id() + "], stmt_name=[" + pt_stmt.getName() + "]");
					};
					if(tag.getAggregation_10_minutes() != null && tag.getAggregation_10_minutes().equals("Y")){
						TagPointAggreationStatement pt_stmt = new TagPointAggreationStatement(tag, "10_MINUTES");
						EPStatement statement = CEPEngineManager.getInstance().getProvider().getEPAdministrator().createEPL(pt_stmt.getEPL(),  pt_stmt.getName());
						statement.addListener(pt_stmt);
						statement.start();
						log.info("Aggregation tag deploy completed for tag_id=[" + tag.getTag_id() + "], stmt_name=[" + pt_stmt.getName() + "]");
					};
					if(tag.getAggregation_30_minutes() != null && tag.getAggregation_30_minutes().equals("Y")){
						TagPointAggreationStatement pt_stmt = new TagPointAggreationStatement(tag, "30_MINUTES");
						EPStatement statement = CEPEngineManager.getInstance().getProvider().getEPAdministrator().createEPL(pt_stmt.getEPL(), pt_stmt.getName());
						statement.addListener(pt_stmt);
						statement.start();
						log.info("Aggregation tag deploy completed for tag_id=[" + tag.getTag_id() + "], stmt_name=[" + pt_stmt.getName() + "]");
					};
					if(tag.getAggregation_1_hours() != null && tag.getAggregation_1_hours().equals("Y")){
						TagPointAggreationStatement pt_stmt = new TagPointAggreationStatement(tag, "1_HOURS");
						EPStatement statement = CEPEngineManager.getInstance().getProvider().getEPAdministrator().createEPL(pt_stmt.getEPL(), pt_stmt.getName());
						statement.addListener(pt_stmt);
						statement.start();
						log.info("Aggregation tag deploy completed for tag_id=[" + tag.getTag_id() + "], stmt_name=[" + pt_stmt.getName() + "]");
					};
						
				}
				
			

			//
		} catch (Exception ex) {
			log.error("Aggregation tag deploy error : tag_id=[" + tag.getTag_id() + "] : " + ex.getMessage(), ex);
		}
	}

	public void undeploy(Tag tag) throws Exception {

		try {

			//
			for(String smt_name : CEPEngineManager.getInstance().getProvider().getEPAdministrator().getStatementNames()){
				if(smt_name.startsWith("PP_TAG_AGGREGATION_" + tag.getTag_id())){
					CEPEngineManager.getInstance().getProvider().getEPAdministrator().getStatement(smt_name).destroy();
					log.debug("Aggregation tag undeploy completed for tag_id=[" + tag.getTag_id() + "]");
				}
			}
			;
			//
		} catch (Exception ex) {
			log.error("Aggregation tag undeploy error : tag_id=[" + tag.getTag_id() + "] : " + ex.getMessage(), ex);
		}
	}

}
