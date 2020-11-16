package lagou.zk.onoffline;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

//提高时间查询服务
public class TimeServer implements Runnable{
    private int port;

    public TimeServer(int port){
        this.port = port;
    }
    @Override
    public void run() {
        try {
            //指定监听的端口
            ServerSocket serverSocket = new ServerSocket(port);
            while (true) {
                Socket socket = serverSocket.accept();
                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(new Date().toString().getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
