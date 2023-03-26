import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import org.junit.Test;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * @author zt
 */

public class Demo {

    @Test
    public void test0001() throws Exception {
        Socket socket = new Socket(InetAddress.getLocalHost(), 999);
        OutputStream outputStream = socket.getOutputStream();
        Person person = new Person();
        person.setTs(new Date());
        outputStream.write(JSON.toJSONBytes(person));
        outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void test0002() {
        Person person = new Person();
        person.setTs(new Date());
        boolean b = System.currentTimeMillis() % 2 == 0;
        if (b) {
            person.setName("zs");
        }else{
            person.setName("ls");
        }
        System.out.println(JSON.toJSONString(person));
    }
}
