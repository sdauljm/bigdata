package lagou;

import lagou.util.StringSerizal;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

/**
 * Hello world!
 *
 */
@SpringBootApplication(exclude={DataSourceAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class, JpaRepositoriesAutoConfiguration.class})
public class App {
    public static void main( String[] args ) {
        SpringApplication.run(App.class,args);

    }
}
