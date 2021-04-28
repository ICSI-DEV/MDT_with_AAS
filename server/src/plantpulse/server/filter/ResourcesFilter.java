package plantpulse.server.filter;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.stream.Collectors;

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

import plantpulse.server.filter.cache.CacheConfigParameter;
import plantpulse.server.filter.cache.ComressionCacheMapFactory;
import plantpulse.server.filter.cache.HTTPCacheHeader;
import plantpulse.server.filter.compressor.YuiCssCompressor;
import plantpulse.server.filter.compressor.YuiJavaScriptCompressor;


/**
 * 웹 스태틱 리소스 필더 (JS 및 CSS 컴프레션 및 캐쉬)
 * 
 * @author leesa
 *
 */
public class ResourcesFilter implements Filter {
	
	private static final Log log = LogFactory.getLog(ResourcesFilter.class);
	
	private static final long CACHE_DAYS = Timestamp.valueOf(LocalDateTime.now().plusDays(365)).getTime();; //365일
	private static final long JS_COMPRESS_TIME_WARN = 1_000;    //500ms

	@Override
	public void init(FilterConfig config) throws ServletException {

	}

	@Override
	public void destroy() {
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		
		InputStream input = null;
		OutputStream output =  null;
		String compressed_output = "";
		try {
			
			//
			String path = ((HttpServletRequest) request).getServletPath();
			path = path.substring(8, path.length());
			String full_path = "/META-INF/web-resources/" + path;
			input = getClass().getResourceAsStream(full_path);
			
			if (input != null) {
				
				if( path.endsWith(".css")){ //CSS
					 response.setContentType("text/css");
					 ((HttpServletResponse) response).setHeader(HTTPCacheHeader.CACHE_CONTROL.getName(), CacheConfigParameter.PRIVATE.getName());
					 ((HttpServletResponse) response).setDateHeader(HTTPCacheHeader.EXPIRES.getName(), System.currentTimeMillis() + CACHE_DAYS);
				        		
					if( ComressionCacheMapFactory.getInstance().getCache().containsKey(path)) {
						compressed_output = ComressionCacheMapFactory.getInstance().getCache().get(path);
					}else {
						 YuiCssCompressor ycc = new YuiCssCompressor();
						 String content = convert(input, Charset.forName("UTF-8"));
						 compressed_output = ycc.compress(content);
						 ComressionCacheMapFactory.getInstance().getCache().put(path, compressed_output);
					};
					 output = response.getOutputStream();
					 output.write(compressed_output.getBytes(Charset.forName("UTF-8")));
					 
				 }else if( path.endsWith(".json")){ //JSON
						 response.setContentType("application/json; charset=utf-8");
						 ((HttpServletResponse) response).setHeader(HTTPCacheHeader.CACHE_CONTROL.getName(), CacheConfigParameter.PRIVATE.getName());
						 ((HttpServletResponse) response).setDateHeader(HTTPCacheHeader.EXPIRES.getName(), System.currentTimeMillis() + CACHE_DAYS);
					        		
						if( ComressionCacheMapFactory.getInstance().getCache().containsKey(path)) {
							compressed_output = ComressionCacheMapFactory.getInstance().getCache().get(path);
						}else {
							 String content = convert(input, Charset.forName("UTF-8"));
							 compressed_output =(content);
							 ComressionCacheMapFactory.getInstance().getCache().put(path, compressed_output);
						};
						 output = response.getOutputStream();
						 output.write(compressed_output.getBytes(Charset.forName("UTF-8")));


				}else if( path.endsWith(".js")){ //JS
					response.setContentType("application/javascript");
					 ((HttpServletResponse) response).setHeader(HTTPCacheHeader.CACHE_CONTROL.getName(), CacheConfigParameter.PRIVATE.getName());
					 ((HttpServletResponse) response).setDateHeader(HTTPCacheHeader.EXPIRES.getName(), System.currentTimeMillis() + CACHE_DAYS);
					 
					if( ComressionCacheMapFactory.getInstance().getCache().containsKey(path)) {
						compressed_output = ComressionCacheMapFactory.getInstance().getCache().get(path);
					}else {
						
						String content = convert(input, Charset.forName("UTF-8"));
						if(
								!path.endsWith("d3.js") 
								&& !path.endsWith("d3.min.js")
								&& !path.endsWith("codemirror.js")
								&& !path.endsWith("codemirror.min.js")
						) { //컴프레션이 너무 느린 JS는 미리 제거
							try {
								long start = System.currentTimeMillis();
								YuiJavaScriptCompressor cjsc = new YuiJavaScriptCompressor();
								compressed_output = cjsc.compress(content);
								long end = System.currentTimeMillis() - start;
								if(end > JS_COMPRESS_TIME_WARN) {
									log.warn("JS Compression slow : path = [" + path + "], compress_time_ms = [" + end + "], file_length = [" + content.length() + "]");
								}
							}catch(Exception ex) {
								log.info("JS Compressoin exception, invalid javascript code : path = [" + path + "]");
								compressed_output = content;
								ComressionCacheMapFactory.getInstance().getCache().put(path, compressed_output);
							}
						}else {
							compressed_output = content;
							ComressionCacheMapFactory.getInstance().getCache().put(path, compressed_output);
						}
					};
					output = response.getOutputStream();
					output.write(compressed_output.getBytes(Charset.forName("UTF-8")));
					
				}else { 
					//IMAGE ETC
					
					 ((HttpServletResponse) response).setHeader(HTTPCacheHeader.CACHE_CONTROL.getName(), CacheConfigParameter.PRIVATE.getName());
					 ((HttpServletResponse) response).setDateHeader(HTTPCacheHeader.EXPIRES.getName(), System.currentTimeMillis() + CACHE_DAYS);
				       
					byte b[] = new byte[input.available()];
					int leng = 0;
					output = response.getOutputStream();
					while ((leng = input.read(b)) > 0) {
						output.write(b, 0, leng);
					};
				};
				
			} else {
				log.warn("Not found web ui static-resources : " + full_path);
			};
		} catch (Exception ex) {
			log.warn("Web static resources filter exception : " + ex.getMessage(), ex);
		} finally {
			if(input != null) input.close();
			if(output != null) output.close();
		};
	}
	

	public String convert(InputStream inputStream, Charset charset) throws IOException {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, charset))) {
			return br.lines().collect(Collectors.joining(System.lineSeparator()));
		}
	}
}
