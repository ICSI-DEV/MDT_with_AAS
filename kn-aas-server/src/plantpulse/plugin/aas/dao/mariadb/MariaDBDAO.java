package plantpulse.plugin.aas.dao.mariadb;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MariaDBDAO {
	
	
	public  void saveData(long timestamp, String aas_id, String json) {
        Connection con = null;
        PreparedStatement pstmt = null;   
        ResultSet rs = null;
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            //
            con = DriverManager.getConnection(
                "jdbc:mariadb://100.100.100.7:3306/dbname",
                "userId",
                "password");
                        
            pstmt = con.prepareStatement("insert into ass_data_repositry (timestamp, aas_id, json) values (" + timestamp + ", '" + aas_id + "', '" + json + "')");
            
            pstmt.executeUpdate();

        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(rs != null) {
                    rs.close(); // 선택 사항
                }
                
                if(pstmt != null) {
                    pstmt.close(); // 선택사항이지만 호출 추천
                }
            
                if(con != null) {
                    con.close(); // 필수 사항
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
