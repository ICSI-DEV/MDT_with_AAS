package plantpulse.cep.domain;

import java.sql.Date;

/**
 * Statement
 * 
 * @author lenovo
 *
 */
public class Statement {

	private String statementName;
	private String statementDesc;

	private String epl;
	private String isPattern;
	private String listener;

	private Date insertDate;
	private Date lastUpdateDate;
	private String status;

	public String getStatementName() {
		return statementName;
	}

	public void setStatementName(String statementName) {
		this.statementName = statementName;
	}

	public String getEpl() {
		return epl;
	}

	public void setEpl(String epl) {
		this.epl = epl;
	}

	public Date getInsertDate() {
		return insertDate;
	}

	public void setInsertDate(Date insertDate) {
		this.insertDate = insertDate;
	}

	public Date getLastUpdateDate() {
		return lastUpdateDate;
	}

	public void setLastUpdateDate(Date lastUpdateDate) {
		this.lastUpdateDate = lastUpdateDate;
	}

	public String getListener() {
		return listener;
	}

	public void setListener(String listener) {
		this.listener = listener;
	}

	public String getIsPattern() {
		return isPattern;
	}

	public void setIsPattern(String isPattern) {
		this.isPattern = isPattern;
	}

	public String getStatementDesc() {
		return statementDesc;
	}

	public void setStatementDesc(String statementDesc) {
		this.statementDesc = statementDesc;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public String toString() {
		return "Statement [statementName=" + statementName + ", statementDesc=" + statementDesc + ", epl=" + epl + ", isPattern=" + isPattern + ", listener=" + listener + ", insertDate=" + insertDate
				+ ", lastUpdateDate=" + lastUpdateDate + ", status=" + status + "]";
	}

}
