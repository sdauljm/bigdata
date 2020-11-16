package lagou.zk.demo;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

public class DataChange {
    public static void main(String[] args) throws InterruptedException {
        ZkClient client = new ZkClient("linux01:2181");
        if(!client.exists("/lagou")){
            client.createPersistent("/lagou","123");
        }
        client.setZkSerializer(new StringSerizal());
        client.subscribeDataChanges("/lagou", new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {
                System.out.println(s+" data is "+o+"now");
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                System.out.println(s+" is deleted!!!");
            }
        });
        Object o = client.readData("/lagou");
        System.out.println(o);

        client.writeData("/lagou","new data");
        Thread.sleep(1000);
        client.delete("/lagou");
        while (true){

        }
    }
}
