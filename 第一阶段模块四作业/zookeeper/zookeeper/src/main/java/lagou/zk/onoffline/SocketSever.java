package lagou.zk.onoffline;

import org.I0Itec.zkclient.ZkClient;

/**
 * 对外提供访问,返回当前时间
 * 并在Zookeeper上注册临时顺序节点
 */
public class SocketSever {
    //注册节点到zookeeper上
    //1.获取zookeeper客户端对象
    private ZkClient zkClient = null;
    private void createZkClient(){
        zkClient  =  new ZkClient("linux01:2181,linux02:2181");
        if(!zkClient.exists("/servers")){
            zkClient.createPersistent("/servers");
        }
    }
    //2.注册临时顺序节点
    private void  register(String ip,String port){
        String result = zkClient.createEphemeralSequential("/servers/server", ip + ":" + port);
        System.out.println("--->>>,服务器["+ip+":"+port+"]向zk注册顺序节点成功");
    }

    public static void main(String[] args) {
         //采用多线程模拟两个服务端
        SocketSever socketSever = new SocketSever();
        socketSever.createZkClient();
        socketSever.register(args[0],args[1]);
        TimeServer timeServer = new TimeServer(Integer.parseInt(args[1]));
        new Thread(new TimeServer(Integer.parseInt(args[1]))).start();
    }
}
