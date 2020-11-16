package lagou.zk.demo;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;

public class ChildChange {
    public static void main(String[] args) throws InterruptedException {

        ZkClient client = new ZkClient("linux01:2181");

        client.subscribeChildChanges("/la-client", new IZkChildListener() {
            @Override
            public void handleChildChange(String s, List<String> list) throws Exception {
                System.out.println(s+"childs:"+list);
            }
        });

         client.createPersistent("/la-client");
         Thread.sleep(1000);
         client.createPersistent("/la-client/c");
         Thread.sleep(1000);
         client.delete("/la-client/c");
         Thread.sleep(1000);
         client.delete("/la-client");
         while (true){

         }
    }

}
