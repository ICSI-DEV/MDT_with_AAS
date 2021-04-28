/*
 *  JDesigner (R) Web Application Development Platform 
 *  Copyright (C) 2010 JDesigner / SangBoo Lee (leesangboo@gmail.com)
 * 
 *  http://www.jdesigner.org/
 *  
 *  Please contact if you require licensing terms other
 *  than GPL-3 for this software.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package plantpulse.server.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

/**
 * 케릭터셋 인코딩 필터
 * 
 * @author leesangboo
 * 
 */
public class EncodingFilter implements Filter {

	// default to ASCII
	private String targetEncoding = "ASCII";

	@Override
	public void init(FilterConfig config) throws ServletException {
		this.targetEncoding = config.getInitParameter("encoding");
	}

	@Override
	public void destroy() {
		targetEncoding = null;
	}

	@Override
	public void doFilter(ServletRequest srequest, ServletResponse sresponse, FilterChain chain) throws IOException, ServletException {

		HttpServletRequest request = (HttpServletRequest) srequest;
		request.setCharacterEncoding(targetEncoding);
		// move on to the next
		chain.doFilter(srequest, sresponse);
	}
}
