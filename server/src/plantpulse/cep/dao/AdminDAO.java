package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AdminDAO  extends CommonDAO {
	
	private static final Log log = LogFactory.getLog(AdminDAO.class);
	
	public void clearMetastore() throws Exception {
		Connection con = null;
		Statement stmt = null;
		try {
			//
			con = super.getConnection();
			
			stmt = con.createStatement();
			stmt.execute(" DELETE FROM mm_sites");
			stmt.execute(" DELETE FROM mm_opcs ");
			stmt.execute(" DELETE FROM mm_tags ");
			stmt.execute(" DELETE FROM mm_alarm ");
			stmt.execute(" DELETE FROM mm_alarm_config ");
			stmt.execute(" DELETE FROM mm_asset_tree ");
			stmt.execute(" DELETE FROM mm_asset_statement ");
			stmt.execute(" DELETE FROM mm_control ");

			stmt.execute(" DELETE FROM mm_dashboard ");
			stmt.execute(" DELETE FROM mm_graph ");
			stmt.execute(" DELETE FROM mm_scada ");
			stmt.execute(" DELETE FROM mm_trigger ");
			stmt.execute(" DELETE FROM mm_trigger_attributes ");
			stmt.execute(" DELETE FROM mm_query_history ");

		} catch (Exception ex) {
			log.error("Metastore clear failed : " + ex.getMessage(), ex);
		} finally {
			if (stmt != null) stmt.close();
			super.closeConnection(con);
		}
	}


	


}
