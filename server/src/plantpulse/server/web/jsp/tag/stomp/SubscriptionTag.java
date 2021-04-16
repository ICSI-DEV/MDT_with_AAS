package plantpulse.server.web.jsp.tag.stomp;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.PageContext;
import javax.servlet.jsp.tagext.SimpleTagSupport;

/**
 * 
 * @author lsb
 *
 */
public class SubscriptionTag extends SimpleTagSupport {

	public static final String __SUBSCRIPTION_LIST = "__SUBSCRIPTION_LIST";

	@Override
	public void doTag() throws JspException, IOException {
		StringWriter sw = new StringWriter();
		getJspBody().invoke(sw);
		String subscription = sw.toString();
		List<String> subscription_list = (List<String>) getJspContext().getAttribute(__SUBSCRIPTION_LIST, PageContext.REQUEST_SCOPE);
		if (subscription_list == null) {
			subscription_list = new ArrayList<String>();
		}
		subscription_list.add(subscription + "\n\n");
		super.getJspContext().setAttribute(__SUBSCRIPTION_LIST, subscription_list, PageContext.REQUEST_SCOPE);
	}

}