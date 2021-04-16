package plantpulse.server.filter.compressor;



import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.googlecode.htmlcompressor.compressor.HtmlCompressor;
import com.yahoo.platform.yui.compressor.CssCompressor;

/**
 * Basic CSS compressor implementation using <a href="http://developer.yahoo.com/yui/compressor/">Yahoo YUI Compressor</a>   
 * that could be used by {@link HtmlCompressor} for inline CSS compression.
 * 
 * @author <a href="mailto:serg472@gmail.com">Sergiy Kovalchuk</a>
 * 
 * @see HtmlCompressor#setCssCompressor(Compressor)
 * @see <a href="http://developer.yahoo.com/yui/compressor/">Yahoo YUI Compressor</a>
 */
public class YuiCssCompressor implements Compressor {
	
	private static final Log log = LogFactory.getLog(YuiCssCompressor.class);
	
	private int lineBreak = -1;
    
    public YuiCssCompressor() {
    }
    
    @Override
	public String compress(String source) {
    	StringWriter result = new StringWriter();
		
		try {
			CssCompressor compressor = new CssCompressor(new StringReader(source));
			compressor.compress(result, lineBreak);
		} catch (IOException e) {
			result.write(source);
			log.error("YUI CSS Compression error : " + e.getMessage(),  e);
		}
		
		return result.toString();
	}

    /**
	 * Returns number of symbols per line Yahoo YUI Compressor
	 * will use during CSS compression. 
	 * This corresponds to <code>--line-break</code> command line option.
	 *   
	 * @return <code>line-break</code> parameter value used for CSS compression.
	 * 
	 * @see <a href="http://developer.yahoo.com/yui/compressor/">Yahoo YUI Compressor</a>
	 */
	public int getLineBreak() {
		return lineBreak;
	}
	/**
	 * Tells Yahoo YUI Compressor to break lines after the specified number of symbols 
	 * during CSS compression. This corresponds to 
	 * <code>--line-break</code> command line option. 
	 * This option has effect only if CSS compression is enabled.
	 * Default is <code>-1</code> to disable line breaks.
	 * 
	 * @param lineBreak set number of symbols per line
	 * 
	 * @see <a href="http://developer.yahoo.com/yui/compressor/">Yahoo YUI Compressor</a>
	 */
	public void setLineBreak(int lineBreak) {
		this.lineBreak = lineBreak;
	}

}