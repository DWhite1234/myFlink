import com.zt.flink.test.utils.DateUtils;
import org.junit.Test;

import java.time.*;
import java.util.Calendar;
import java.util.Date;

/**
 * @author zt
 */
public class Demo {

    @Test
    public void Test001() {
        Integer a = -1000;
        Integer b = 1000;
        System.out.println(a);
        System.out.println(b);
        System.out.println(a+b);
    }

    @Test
    public void Test002() {
        Integer a = -1000;
        System.out.println(a-1000);
    }

    @Test
    public void Test003() {
        Calendar instance = Calendar.getInstance();
        instance.set(2023, 1, 1);
        Date time = instance.getTime();
        System.out.println(DateUtils.dateInterval(time));
    }

    @Test
    public void Test004() {
        System.out.println(DateUtils.dateInterval("2023-01-06"));
        System.out.println(DateUtils.daysAfter("2023-01-06",1));
    }

}
