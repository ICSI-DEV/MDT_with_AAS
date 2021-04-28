package plantpulse.plugin.opcua.utils;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * SVGUtils
 * 
 * @author lsb
 *
 */
public class SVGUtils {
	
	private static final Log log = LogFactory.getLog(SVGUtils.class);
	
	public static String getSVG(String path) {
		try{
			return FileUtils.readFileToString(new File(path));
		}catch(Exception ex){
			log.error("SVG 파일 로드 실패" , ex);
		}
		return null;
    };


}
