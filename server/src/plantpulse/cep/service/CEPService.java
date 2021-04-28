package plantpulse.cep.service;

import java.util.List;

import plantpulse.cep.dao.CEPServiceDAO;
import plantpulse.cep.domain.Event;
import plantpulse.cep.domain.Statement;

public class CEPService {

	private CEPServiceDAO dao = new CEPServiceDAO();

	// EVENT
	public List<Event> getEventList() throws Exception {
		return dao.getEventList();
	}

	public Event getEvent(String event_name) throws Exception {
		return dao.getEvent(event_name);
	}

	public void saveEvent(Event event) throws Exception {
		dao.saveEvent(event);
	}

	public void updateEvent(Event event) throws Exception {
		dao.updateEvent(event);
	}

	public void deleteEvent(String event_name) throws Exception {
		dao.deleteEvent(event_name);
	}

	// STATEMENT
	public List<Statement> getStatementList() throws Exception {
		return dao.getStatementList();
	}

	public Statement getStatement(String statementName) throws Exception {
		return dao.getStatement(statementName);
	}

	public void saveStatement(Statement statement) throws Exception {
		dao.saveStatement(statement);
	}

	public void updateStatement(Statement statement) throws Exception {
		dao.updateStatement(statement);
	}

	public void deleteStatement(String statementName) throws Exception {
		dao.deleteStatement(statementName);
	}

}
