package plantpulse.server.mvc.admin;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.dao.AdminDAO;
import plantpulse.cep.engine.scheduling.job.TagPointArchiveJob;
import plantpulse.cep.service.AlarmService;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.server.filter.cache.ComressionCacheMapFactory;
import plantpulse.server.mvc.JSONResult;

/**
 * 관리자 도구
 * @author leesa
 *
 */
@Controller
public class AdminController {

	private static final Log log = LogFactory.getLog(AdminController.class);

	@RequestMapping(value = "/admin/tool/archive", method = RequestMethod.GET)
	public @ResponseBody String archive(Model model, HttpServletRequest request) throws Exception {

		//
		JSONResult json = new JSONResult();
		try {
			TagPointArchiveJob aj = new TagPointArchiveJob();
			aj.run();
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("아카이브 실행을 완료하였습니다.");
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("아카이브 실행 도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e.getMessage(), e);
		}
		return json.toString();
	}

	@RequestMapping(value = "/admin/tool/alarm/clear", method = RequestMethod.GET)
	public @ResponseBody String alarmclear(Model model, HttpServletRequest request) throws Exception {
		//
		JSONResult json = new JSONResult();
		try {
			AlarmService alarm_service = new AlarmService();
			alarm_service.clear();
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("알람 삭제를 완료하였습니다.");
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("아카이브 실행 도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e.getMessage(), e);
		}
		return json.toString();
	}

	
	@RequestMapping(value = "/admin/tool/static-cache/clear", method = RequestMethod.GET)
	public @ResponseBody String staticResource(Model model, HttpServletRequest request) throws Exception {
		//
		JSONResult json = new JSONResult();
		try {
			ComressionCacheMapFactory.getInstance().getCache().clear();
			
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("컴프레션 캐쉬 제거를 완료하였습니다.");
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("컴프레션 캐쉬 제거 실행 도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e.getMessage(), e);
		}
		return json.toString();
	}
	

	@RequestMapping(value = "/admin/tool/metastore/clear", method = RequestMethod.GET)
	public @ResponseBody String metastoreClear(Model model, HttpServletRequest request) throws Exception {
		//
		JSONResult json = new JSONResult();
		try {
			AdminDAO dao = new AdminDAO();
			dao.clearMetastore();
			
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("메타스토어 클리어를 완료하였습니다.");
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("메타스토어 클리어 실행 도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e.getMessage(), e);
		}
		return json.toString();
	}
	
	
	@RequestMapping(value = "/admin/tool/replication-factor/change", method = RequestMethod.GET)
	public @ResponseBody String replicatoinFactorChange(Model model, HttpServletRequest request) throws Exception {

		//
		JSONResult json = new JSONResult();
		try {
			
			//
		    StorageClient dao = new StorageClient();
		    dao.forInsert().setupReplicationFactor(3);

		    //
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("클러스터 복제 팩터 변경을 완료하였습니다. 노드 복구를 실행하십시오. [./node-repair.sh]");
			
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("클러스터 복제 팩터 변경 도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e.getMessage(), e);
		}
		return json.toString();
	};


}
