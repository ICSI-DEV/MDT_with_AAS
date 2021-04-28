package plantpulse.server.mvc.stream;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.StreamService;
import plantpulse.cep.service.TagService;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.util.DateUtils;

@Controller
public class StreamController {

	private static final Log log = LogFactory.getLog(StreamController.class);

	@Autowired
	private StreamService stream_service;

	@Autowired
	private SiteService site_service;

	@Autowired
	private TagService tag_service;

	@RequestMapping(value = "/stream/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		String search_date_from = DateUtils.currDateByMinus10Minutes();
		String search_date_to = DateUtils.currDateBy24();
		model.addAttribute("total_log_count", 0);
		model.addAttribute("search_date_from", search_date_from);
		model.addAttribute("search_date_to", search_date_to);

		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");
		return "stream/index";
	}

	@RequestMapping(value = "/stream/tag/{tag_id}", method = RequestMethod.GET)
	public String tag(Model model, @PathVariable String tag_id, HttpServletRequest request) throws Exception {
		Tag tag = tag_service.selectTag(tag_id);
		model.addAttribute("tag", tag);
		return "stream/tag";
	}

}
