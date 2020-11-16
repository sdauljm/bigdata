package hbase.practice;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class DeleteUtil extends BaseRegionObserver {
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException, IOException {
        final HTableInterface relation = e.getEnvironment().getTable(TableName.valueOf("relation"));
        //获取到所有的列族
        final NavigableMap<byte[], List<Cell>> colFamilys= delete.getFamilyCellMap();
        final Set<Map.Entry<byte[], List<Cell>>> entries = colFamilys.entrySet();
        for (Map.Entry<byte[], List<Cell>> entry : entries) {
            System.out.println(Bytes.toString(entry.getKey()));
            final List<Cell> cells = entry.getValue();
            for (Cell cell : cells) {
                final byte[] row = CellUtil.cloneRow(cell);
                final byte[] column = CellUtil.cloneQualifier(cell);//获取所有的列
                final boolean flag = relation.exists(new Get(column).addColumn(Bytes.toBytes("friends"), row));
                if(flag){
                    //判断要删除的元素是否存在,若删除则执行删除操作
                    final Delete myDelete = new Delete(column).addColumn(Bytes.toBytes("friends"), row);
                    relation.delete(myDelete);
                }
            }
        }
    }
}
