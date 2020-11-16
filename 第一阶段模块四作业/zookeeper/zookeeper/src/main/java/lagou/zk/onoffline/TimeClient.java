package lagou.zk.onoffline;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

//提高时间查询服务
public class TimeClient {
  private List<String> servers = new ArrayList<>();
    private ZkClient zkClient = null;
    private void createZkClient() {
        zkClient = new ZkClient("linux01:2181,linux02:2181");
        List<String> children = zkClient.getChildren("/servers");
        for (String child : children) {
            Object o = zkClient.readData("/servers/" + child);
            servers.add((String) o);
        }
        zkClient.subscribeChildChanges("/servers", new IZkChildListener() {
            @Override
            public void handleChildChange(String s, List<String> list) throws Exception {
                System.out.println("最新的服务节点为:" + list);
                List<String> infos = new ArrayList();
                for (String info : list) {
                     infos.add((String)zkClient.readData("/servers/" + info));
                }
                servers = infos
                ;
            }
        });
    }
       public void request() throws IOException {
           Random random = new Random();
           int i = random.nextInt(servers.size());
           String s = servers.get(i);
           System.out.println(s);
           String[] split = s.split(":");
           Socket socket = new Socket(split[0], Integer.parseInt(split[1]));
           OutputStream outputStream = socket.getOutputStream();
           InputStream inputStream = socket.getInputStream();
           outputStream.write("query time".getBytes());
           outputStream.flush();
           byte[] data = new byte[1024];
           inputStream.read(data);
           System.out.println("服务器 "+s+" "+new String(data));
           outputStream.close();
           inputStream.close();
           socket.close();
       }

    public static void main(String[] args) throws IOException, InterruptedException {
        TimeClient client = new TimeClient();
        client.createZkClient();
        while (true){
            try {
                client.request();
            }catch (Exception e){
                System.out.println("连接被拒绝");
                continue;
            }
            Thread.sleep(2000);
        }
    }

}
