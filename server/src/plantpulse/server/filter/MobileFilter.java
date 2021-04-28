package plantpulse.server.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.PatternMatchUtils;

import plantpulse.server.mvc.util.SessionUtils;

/**
 * MobileFilter
 * 
 * @author lsb
 *
 */
public class MobileFilter implements Filter {

	private static final Log log = LogFactory.getLog(MobileFilter.class);

	private static final String[] MOBILE_CHECK_PATTERN = new String[] { "/m/dashboard*", "/m/alarm*" };

	@Override
	public void init(FilterConfig config) throws ServletException {
	}

	@Override
	public void destroy() {

	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		String contextPath = ((HttpServletRequest) request).getServletContext().getContextPath();
		String uri = ((HttpServletRequest) request).getServletPath();
		if (PatternMatchUtils.simpleMatch(MOBILE_CHECK_PATTERN, uri)) {
			log.debug("Security check for URI=[" + uri + "]");
			//
			if (SessionUtils.isLogined((HttpServletRequest) request)) {
				//
				chain.doFilter(request, response);
				//
			} else {
				((HttpServletResponse) response).sendRedirect(contextPath + "/m");
			}
		} else {
			chain.doFilter(request, response);
		}

	}

}
