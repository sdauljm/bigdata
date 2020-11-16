package lagou.controller;

import lagou.config.DataSourceConfig;
import org.I0Itec.zkclient.ZkClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@RestController
public class MyController {
    @Autowired
    private DataSourceConfig dataSourceConfig;
    @RequestMapping("mysql")
    public String mysql() throws SQLException {
        Connection connection = dataSourceConfig.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select name from user");
        while(resultSet.next()){
            String name = resultSet.getString("name");
            System.out.println(name);
        }
        return connection.getMetaData().getURL();
    }
}
