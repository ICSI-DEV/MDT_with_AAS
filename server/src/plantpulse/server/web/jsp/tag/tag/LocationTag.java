package plantpulse.server.web.jsp.tag.tag;

import java.io.IOException;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.JspWriter;
import javax.servlet.jsp.tagext.SimpleTagSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.service.support.tag.TagLocation;

/**
 * LocationTag
 * 
 * @author lsb
 *
 */
public class LocationTag extends SimpleTagSupport {

	private static final Log log = LogFactory.getLog(LocationTag.class);

	private String tag_id = "";

	private boolean show_tag_name = true;

	@Override
	public void doTag() throws JspException, IOException {
		JspWriter out = getJspContext().getOut();
		TagLocation location = new TagLocation();
		out.println(location.getLocation(tag_id, show_tag_name));
	}

	public String getTag_id() {
		return tag_id;
	}

	public void setTag_id(String tag_id) {
		this.tag_id = tag_id;
	}

	public boolean isShow_tag_name() {
		return show_tag_name;
	}

	public void setShow_tag_name(boolean show_tag_name) {
		this.show_tag_name = show_tag_name;
	}

	

}
