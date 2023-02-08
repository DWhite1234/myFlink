import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zt
 */
@Slf4j
public class TestDemo {

    @Test
    public void sendMysql() throws Exception {
        int id = 6000;
        int count = 0;
        String url = "jdbc:mysql://10.211.55.101:3306/mydb?serverTimezone=Asia/Shanghai&useSSL=false";
        String username = "root";
        String password = "123456";
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection(url, username, password);
        int num = 0;
        while (true) {
            String sql = "insert into student values(" + (++id) + ",'zs','2022-10-12 12:00:00','男')";
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            boolean execute = preparedStatement.execute();
            num++;
            if (num >= 5) {
                conn.commit();
                num = 0;
            }
            count++;
            log.info("成功发送条数:{}",count);
        }

    }


    @Test
    public void sendOracle() throws Exception {
        int id = 1;
        int count = 0;
        String url = "jdbc:oracle:thin:@172.1.2.41:1521:helowin?serverTimezone=Asia/Shanghai&useSSL=false";
        String username = "SDC_TEST";
        String password = "666666";
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection conn = DriverManager.getConnection(url, username, password);
        while (id>=2000) {
//            String sql = "update student set s_sex = '456' , s_birth = "+System.currentTimeMillis()+" where s_id = "+(++id);
            String sql = "insert into SDC_TEST.student values(" + (++id) + ",'zs',1,'男',"+System.currentTimeMillis()+")";
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.execute();
            System.out.println("成功发送条数:"+(count++));
        }
    }
}
