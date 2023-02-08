import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author zt
 */
@Slf4j
public class JDBCSend {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        int id = 10;
        int count = 0;
        String url = "jdbc:mysql://10.211.55.101:3306/mydb?serverTimezone=Asia/Shanghai&useSSL=false";
        String username = "root";
        String password = "123456";
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection(url, username, password);
        while (true) {
            String sql = "insert into student_datax values(" + (++id) + ",'zs','18','1000','2022-10-12 12:00:00')";
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            boolean execute = preparedStatement.execute();
            if (execute) {
                conn.commit();
            }
            count++;
            log.info("成功发送条数:{}",count);
        }
    }

}
