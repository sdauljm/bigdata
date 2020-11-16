package lagou.zk.demo;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class StringSerizal implements ZkSerializer {
    @Override
    public byte[] serialize(Object o) throws ZkMarshallingError {
        return ((String)o).getBytes();
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return new String(bytes);
    }
}
