package plantpulse.cep.service;

import java.util.List;
import java.util.UUID;

import plantpulse.cep.dao.TokenDAO;
import plantpulse.cep.domain.Token;
import plantpulse.cep.token.TokenMapFactory;

public class TokenService {

	private TokenDAO dao = new TokenDAO();

	public boolean checkToken(String ip, String token) throws Exception {
		return dao.checkToken(ip, token);
	}

	public List<Token> getTokenList() throws Exception {
		return dao.getTokenList();
	}

	public Token getToken(String token_id) throws Exception {
		return dao.getToken(token_id);
	}

	public void saveToken(Token token) throws Exception {
		dao.saveToken(token);
	}

	public void updateToken(Token token) throws Exception {
		dao.updateToken(token);
	}

	public void deleteToken(String token_id) throws Exception {
		dao.deleteToken(token_id);
	}
	
	public String generateToken() {
		 UUID uuid = UUID.randomUUID();
	     String token = uuid.toString();
		return token;
	}
	
	public void deploy() throws Exception {
		TokenMapFactory.getInstance().clear();
		List<Token> token_list = this.getTokenList();
		if(token_list != null) {
			for(int i=0; i < token_list.size(); i++) {
				Token token = token_list.get(i);
				TokenMapFactory.getInstance().addToken(token.getIp(), token.getToken());
			}
		};
	};
}
