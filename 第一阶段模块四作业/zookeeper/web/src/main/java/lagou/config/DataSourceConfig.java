package lagou.config;

import com.zaxxer.hikari.HikariDataSource;
import lagou.util.StringSerizal;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Configuration
public class DataSourceConfig {
    public  volatile DataSource dataSource;
    private ZkClient zkClient = new ZkClient("linux01:2181");
    @Bean
    public DataSource getDataSource(){
        initDataSource();
        new Thread(new MyTask()).start();
        return dataSource;
    }
    class MyTask implements Runnable{

        @Override
        public void run() {
            zkClient.subscribeDataChanges("/datasource", new IZkDataListener() {
                @Override
                public void handleDataChange(String s, Object o) throws Exception {
                    System.out.println(s+" data is "+o);
                    ((HikariDataSource)dataSource).close();
                    initDataSource();
                }

                @Override
                public void handleDataDeleted(String s) throws Exception {
                    System.out.println(s+" is deleted!!!");
                }
            });
            while (true){

            }
        }
    }
    public void  initDataSource(){
        HikariDataSource source = new HikariDataSource();
        zkClient.setZkSerializer(new StringSerizal());
        String config = zkClient.readData("/datasource");
        String[] split = config.split(" ");
        source.setJdbcUrl(split[0].substring(4));
        source.setUsername(split[1].split(":")[1]);
        source.setPassword(split[2].split(":")[1]);
        source.setReadOnly(true);
        source.setAutoCommit(true);
        source.setConnectionTimeout(30000);
        source.setMaximumPoolSize(50);
        source.setMinimumIdle(10);
        dataSource = source;
    }
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
