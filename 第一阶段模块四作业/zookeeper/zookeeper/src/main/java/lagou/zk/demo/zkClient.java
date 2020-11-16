package lagou.zk.demo;

import org.I0Itec.zkclient.ZkClient;

public class zkClient {
    public static void main(String[] args) {
        //1.获取zk客户端
        ZkClient client = new ZkClient("linux01:2181");
        System.out.println("zk client is ready");
        //当节点已存在时会抛出异常 KeeperException$NodeExistsException
        //当父节点不存在时会抛出异常  KeeperException$NoNodeException
        //client.createPersistent("/la-client/la-c");
        //递归创建节点
        client.createPersistent("/la-client/c8",true);
        System.out.println("创建节点成功");
        //只能删除空节点,当用于删除非空节点时会抛出异常
        //client.delete("/la-client");
        //递归删除节点
      // client .deleteRecursive("/la-client");
    }
}
