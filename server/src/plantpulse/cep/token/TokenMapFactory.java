package plantpulse.cep.token;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Maps;

/**
 * 
 * TokenMapFactory
 * 
 * @author leesa
 *
 */
public class TokenMapFactory {
	
	private static final Log log = LogFactory.getLog(TokenMapFactory.class);

	private static class TokenMapFactoryHolder {
		static TokenMapFactory instance = new TokenMapFactory();
	}

	public static TokenMapFactory getInstance() {
		return TokenMapFactoryHolder.instance;
	}
	
	private Map<String, String> token_map = Maps.newConcurrentMap();

	public Map<String, String> getTokenMap() {
		return token_map;
	}

	public void addToken(String ip, String token) {
		this.token_map.put(ip, token);
	}
	
	
	public void clear() {
		this.token_map = Maps.newConcurrentMap();
	}
	

}
