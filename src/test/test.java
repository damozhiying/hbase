package test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import hbase.HbaseUtil;

public class test {
    public static void main(String[] args) {
        try {
            String hadoop_home = System.getProperty("user.dir") + "\\hadoop_home";
            System.setProperty("hadoop.home.dir", hadoop_home);
            HbaseUtil.init();
            listTables();
            createTable();
            listTables();
            getFamily();
            addFamily();
            deleteFamily();
            deleteTable("tianhao_test");
            insertData();
            getData();
            deleteData();
            getData();
            System.out.println("Hbase Demo演示结束，按任意键删除所建测试表并退出...");
            System.in.read();
            System.out.println();
            deleteTable("tianhao_temp");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void listTables() {
        System.out.println("查看所有表：hbase> list");
        List<Map<String, Object>> list = HbaseUtil.getAllTableList();
        for (Map<String, Object> map : list) {
            System.out.print(map.get("table_name")+" ");
        }
        System.out.println();
        System.out.println();
    }
    
    private static void createTable() {
        System.out.println("创建表“tianhao_temp”和“tianhao_test”...");
        HbaseUtil.createTable("tianhao_test", new String[]{"familyone"});
        HbaseUtil.createTable("tianhao_temp", new String[]{"info"});
        System.out.println();
    }
    
    private static void getFamily() {
        System.out.println("查看表“tianhao_test”的列族...");
        List<Map<String, Object>> list = HbaseUtil.getFamilyList("tianhao_test");
        for (Map<String, Object> map : list) {
            System.out.print(map.get("name")+" ");
        }
        System.out.println();
        System.out.println();
    }
    
    private static void addFamily() {
        System.out.println("为表“tianhao_test”添加列族[familytwo]和[familythree]...");
        if(HbaseUtil.addHbaseFamily("tianhao_test", new String[]{"familytwo","familythree"})){
            System.out.println("Add Hbase Family successfully！");
        }else{
            System.out.println("Add Hbase Family failed！");
        }
        System.out.println();
        getFamily();
    }
    
    private static void deleteFamily() {
        System.out.println("为表“tianhao_test”删除列族[familythree]...");
        if(HbaseUtil.deleteHbaseFamily("tianhao_test", new String[]{"familythree"})){
            System.out.println("Delete Hbase Family successfully！");
        }else{
            System.out.println("Delete Hbase Family failed！");
        }
        System.out.println();
        getFamily();
    }
    
    private static void deleteTable(String tableName) {
        System.out.println("删除表“"+tableName+"”...");
        HbaseUtil.deleteTable(tableName);
        System.out.println();
        listTables();
    }
    
    private static List<Map<String, Object>> createTestData() {
        List<Map<String, Object>> list = new ArrayList<>();
        String[] xm = new String[]{"张三","李四","王五","赵六","小小"};
        String[] xb = new String[]{"男","女","女","男","女"};
        String[] wbmc = new String[]{"A网吧","B网吧","C网吧","D网吧","E网吧"};
        String[] lgmc = new String[]{"A旅馆","B旅馆","C旅馆","D旅馆","E旅馆"};
        String[] rzsj = new String[]{"1993-04-10 12:00:05","1993-01-15 01:00:05","1993-10-10 22:06:05","1993-08-10 02:00:00","1993-05-29 01:27:05"};
        //上网信息
        for (int i = 0; i < 5; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("xm", xm[i]);
            map.put("xb", xb[i]);
            map.put("wbmc", wbmc[i]);
            list.add(map);
        }
        //住宿信息
        for (int i = 0; i < 5; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("xm", xm[i]);
            map.put("xb", xb[i]);
            map.put("lgmc", lgmc[i]);
            map.put("rzsj", rzsj[i]);
            list.add(map);
        }
        Map<String, Object> map = new HashMap<>();
        map.put("xm", "我");
        map.put("action", "打酱油");
        list.add(map);
        return list;
    }
    
    private static void insertData() {
        System.out.println("向表“tianhao_temp”中插入数据...");
        if(HbaseUtil.insert("tianhao_temp", "info", createTestData())){
            System.out.println("Insert Data into Hbase Table successfully!");
        }else{
            System.out.println("Insert Data into Hbase Table failed!");
        }
        System.out.println();
    }
    
    private static void getData() {
        System.out.println("获取表“tianhao_temp”中的数据...");
        List<Map<String, String>> list = HbaseUtil.getByPage("tianhao_temp", 11, "0000000000");
        for (Map<String, String> map : list) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                System.out.print(entry.getKey()+"=>"+entry.getValue()+"; "); 
            }
            System.out.println();
        }
        System.out.println();
    }
    
    private static void deleteData() {
        System.out.println("删除表“tianhao_temp”中的最后一条数据...");
        if(HbaseUtil.deleteRow("tianhao_temp", "0000000010")){
            System.out.println("Delete Hbase Table Row successfully!");
        }else{
            System.out.println("Delete Hbase Table Row failed!");
        }
        System.out.println();
    }
}
