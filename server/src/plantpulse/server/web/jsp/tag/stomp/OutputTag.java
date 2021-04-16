package plantpulse.server.web.jsp.tag.stomp;

import java.io.IOException;
import java.util.List;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.JspWriter;
import javax.servlet.jsp.PageContext;
import javax.servlet.jsp.tagext.SimpleTagSupport;

public class OutputTag extends SimpleTagSupport {

	@Override
	public void doTag() throws JspException, IOException {
		JspWriter out = getJspContext().getOut();
		List<String> subscription_list = (List<String>) getJspContext().getAttribute(SubscriptionTag.__SUBSCRIPTION_LIST, PageContext.REQUEST_SCOPE);
		for (int i = 0; subscription_list != null && i < subscription_list.size(); i++) {
			out.println(subscription_list.get(i));
		}
	}
}
