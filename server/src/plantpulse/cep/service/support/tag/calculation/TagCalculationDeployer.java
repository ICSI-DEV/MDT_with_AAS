package plantpulse.cep.service.support.tag.calculation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.domain.Query;
import plantpulse.cep.service.QueryService;
import plantpulse.domain.Tag;

/**
 * TagCalculationDeployer
 * @author lsb
 *
 */
public class TagCalculationDeployer {

	private static final Log log = LogFactory.getLog(TagCalculationDeployer.class);

	/**
	 * 
	 * @param tag
	 * @param alarm_config
	 * @throws Exception
	 */
	public void deploy(Tag tag) throws Exception {
		try {
			//
			TagDAO dao = new TagDAO();
			tag = dao.selectTagInfoWithSiteAndOpc(tag.getTag_id());

			//
			QueryService query_service = new QueryService();
			Query query = new Query();
			query.setId("CACULCATION_" + tag.getTag_id());
			query.setEpl(tag.getCalculation_eql());
			query_service.remove(query);
			query_service.run(query, new CalculationTagEventListener(tag));

			//
			log.info("Calculation tag deploy completed for tag_id=[" + tag.getTag_id() + "], eql=[" + tag.getCalculation_eql() + "]");

		} catch (Exception ex) {
			log.error("Calculation tag deploy error : tag_id=[" + tag.getTag_id() + "] : " + ex.getMessage(), ex);
		}
	}

	
	public void undeploy(Tag tag) throws Exception {

		try {
			//
			QueryService query_service = new QueryService();
			Query query = new Query();
			query.setId("CACULCATION_" + tag.getTag_id());
			query.setEpl(tag.getCalculation_eql());
			query_service.remove(query);

			//
			log.debug("Calculation tag undeploy completed for tag_id=[" + tag.getTag_id() + "]");

			//
		} catch (Exception ex) {
			log.error("Calculation tag undeploy error : tag_id=[" + tag.getTag_id() + "] : " + ex.getMessage(), ex);
		}
	}

}
