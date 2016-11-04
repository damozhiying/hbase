package hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes; 
 
public class HbaseUtil { 
 
    private static Configuration conf;
    private static Connection conn;
    private static long rowCount = 0;
    
    public static void init(){ 
        try {
            System.out.println("创建连接中...");
            conf = HBaseConfiguration.create(); 
            conf.set("hbase.zookeeper.property.clientPort", "2181"); 
            conf.set("hbase.zookeeper.quorum", "10.106.1.172"); 
            conf.set("zookeeper.znode.parent", "/hbase-unsecure");
            conn = ConnectionFactory.createConnection(conf);
            System.out.println("创建连接成功！");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("创建连接失败！");
        }
    }
    
    /*============================== Hbase表、列族操作 ==============================*/
    
    /**
     * 创建表
     * @author tianhao
     * @param tableName
     * @param familes
     */
    public static boolean createTable(String tableName, String[] familes) {
        try {
            Admin admin = conn.getAdmin();
            TableName tName = TableName.valueOf(tableName);
            if(admin.tableExists(tName)){
                System.out.println("table "+tableName+" is already exists!");
            }else{
                HTableDescriptor htd = new HTableDescriptor(tName);
                for (String family : familes) {
                    htd.addFamily(new HColumnDescriptor(family));
                }
                admin.createTable(htd);
                System.out.println("table "+tableName+" is created successfully!");
            }
            admin.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 删除表
     * @author tianhao
     * @param tableName
     */
    public static boolean deleteTable(String tableName) {
        try {
            Admin admin = conn.getAdmin();
            TableName tName = TableName.valueOf(tableName);
            if(admin.tableExists(tName)){
                admin.disableTable(tName);
                admin.deleteTable(tName);
                System.out.println("table "+tableName+" is deleted successfully!");
            }else{
                System.out.println("table "+tableName+" is not exists!");
            }
            admin.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 获取所有表
     * @author tianhao
     * @return
     */
    public static List<Map<String,Object>> getAllTableList(){
        List<Map<String,Object>> tableList = new ArrayList<Map<String,Object>>();
        try {
            
            Admin admin = conn.getAdmin();
            HTableDescriptor[] tables = admin.listTables();
            
            for (HTableDescriptor htd : tables) {
                Map<String,Object> map = new HashMap<String,Object>();
                map.put("table_name", htd.getNameAsString());
                tableList.add(map);
            }
            
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();   
        } 
        return tableList;
    }
    
    /**
     * 添加列族
     * @author tianhao
     * @param tableName
     * @param families
     */
    public static boolean addHbaseFamily(String tableName,String[] families){
        try {
            Admin admin = conn.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            for (String family : families) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
                admin.addColumn(TableName.valueOf(tableName), columnDescriptor);
            }
            admin.enableTable(TableName.valueOf(tableName));
            admin.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 删除列族
     * @author tianhao
     * @param tableName
     * @param families
     */
    public static boolean deleteHbaseFamily(String tableName,String[] families){
        try {
            Admin admin = conn.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            for (String family : families) {
                admin.deleteColumn(TableName.valueOf(tableName), family.getBytes());//删除列族
            }
            admin.enableTable(TableName.valueOf(tableName));
            admin.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 获取某张表的列族信息
     * @author tianhao
     * @param tableName
     * @return
     */
    public static List<Map<String,Object>> getFamilyList(String tableName){
        List<Map<String,Object>> families = new ArrayList<Map<String,Object>>();
        try {
            
            Admin admin = conn.getAdmin();
            HTableDescriptor[] tables = admin.listTables();
            
            for (HTableDescriptor htd : tables) {
                if(tableName.equals(htd.getNameAsString())){
                    Iterator<HColumnDescriptor> it = htd.getFamilies().iterator();
                    while(it.hasNext()){
                        Map<String,Object> fmap = new HashMap<String,Object>();
                        HColumnDescriptor temp = it.next();
                        fmap.put("name",temp.getNameAsString());
                        fmap.put("ttl",temp.getTimeToLive());
                        fmap.put("versions",temp.getMaxVersions());
                        fmap.put("min_versions",temp.getMinVersions());
                        families.add(fmap);
                    }
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();   
        } 
        return families;
    }
    
    /*============================== Hbase表数据操作 ==============================*/
    
    /**
     * 写入数据
     * @author tianhao
     * @param tableName
     * @param family
     * @param data
     */
    public static boolean insert(String tableName, String family, List<Map<String, Object>> data) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            for (Map<String, Object> info : data) {
                Put put = new Put(Bytes.toBytes(String.format("%010d", rowCount)));
                for (Map.Entry<String, Object> entry : info.entrySet()) {
                    put.addColumn(Bytes.toBytes(family),Bytes.toBytes(entry.getKey()),Bytes.toBytes(String.valueOf(entry.getValue())));
                }
                table.put(put);
                rowCount++;
            }
            table.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 根据rowKey删除某行记录
     * @author tianhao
     * @param tableName
     * @param rowkey
     * @return
     */
    public static boolean deleteRow(String tableName, String rowkey){
        try{
            Delete delete = new Delete(rowkey.getBytes());
            conn.getTable(TableName.valueOf(tableName)).delete(delete);
            return true;
        } catch (Exception e) { 
            e.printStackTrace();
            return false;
        } 
    }
    
    /**
     * 根据rowKey查询数据
     * @author tianhao
     * @param tableName
     * @param rowKeys
     * @return
     */
    public static List<Map<String,String>> getRows(String tableName, List<String> rowKeys){
        List<Map<String,String>> list = new ArrayList<Map<String,String>>();
        try {
            List<Get> getList = new ArrayList<Get>();
            for(String str:rowKeys){
                Get get = new Get(str.getBytes());
                getList.add(get);
            }
            Result[] rs = conn.getTable(TableName.valueOf(tableName)).get(getList);
            for (Result result : rs){
                list.add(convertResultToMap(result));
            }
        } catch (Exception e) { 
            e.printStackTrace();
        }    
        return list;
    }
    
    /**
     * 分页查询
     * @author tianhao
     * @param tableName
     * @param count
     * @param startRow
     * @return
     */
    public static List<Map<String, String>> getByPage(String tableName, Integer count, String startRow) {
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        ResultScanner scanner = null;
        try {
            Scan scan = new Scan();
            Filter filter = new PageFilter(count);
            FilterList filterList = new FilterList();
            filterList.addFilter(filter);
            scan.setFilter(filterList);
            if ((startRow != null) && (startRow.length() > 0))
                scan.setStartRow(startRow.getBytes());
            scanner = conn.getTable(TableName.valueOf(tableName)).getScanner(scan);
            for (Result result : scanner) {
                list.add(convertResultToMap(result));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return list;
    }
    
    /**
     * 将result转变为map
     * @author tianhao
     * @param result
     * @return
     */
    private static Map<String,String> convertResultToMap(Result result){
        Map<String,String> map = new HashMap<String,String>();
        try{
            if(result != null){
                Cell[] cells = result.rawCells();
                for(Cell cell : cells){
                    map.put("rowkey", new String(CellUtil.cloneRow(cell),"utf-8"));
                    map.put(new String(CellUtil.cloneQualifier(cell),"utf-8"), new String(CellUtil.cloneValue(cell),"utf-8"));
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return map;
    }
}