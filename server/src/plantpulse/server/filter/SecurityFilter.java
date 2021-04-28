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

import plantpulse.cep.service.support.security.RoleAndSecurity;
import plantpulse.cep.service.support.security.SecurityContext;
import plantpulse.cep.service.support.security.SecurityTools;
import plantpulse.server.mvc.util.SessionUtils;

public class SecurityFilter implements Filter {

	private static final Log log = LogFactory.getLog(SecurityFilter.class);

	private String[] checkPattern = new String[] { "/performance/*", "/query/*" };

	@Override
	public void init(FilterConfig config) throws ServletException {
		this.checkPattern = config.getInitParameter("check-pattern").split(",");
	}

	@Override
	public void destroy() {
		checkPattern = null;
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		String contextPath = ((HttpServletRequest) request).getServletContext().getContextPath();
		String uri = ((HttpServletRequest) request).getServletPath();
		// String uri = ((HttpServletRequest) request).getRequestURI();
		if (PatternMatchUtils.simpleMatch(checkPattern, uri)) {
			log.debug("Security check for URI=[" + uri + "]");
			//
			if (SessionUtils.isLogined((HttpServletRequest) request)) {
				//
				SecurityTools tools = new SecurityTools();
				RoleAndSecurity rs = tools.getRoleAndSecurity((HttpServletRequest) request);
				SecurityContext.setRoleAndSecurity(rs);
				//
				chain.doFilter(request, response);
				//
				SecurityContext.removeRoleAndSecurity();
				//
			} else {
				((HttpServletResponse) response).sendRedirect(contextPath + "/login/logout");
			}
		} else {
			chain.doFilter(request, response);
		}

	}

}
