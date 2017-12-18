package parser;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.FileUtils;
import org.bson.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class ExportMongoinfo {
    //数据库和连接
    public static final String  DATABASE = "mapping";//"mss_sync_db"
    public static final String  COLLECTION = "mSSHRInfomation";
//    public static final String  DATABASE = "repository_d_telecom";
//    public static final String  COLLECTION = "user";

    //查询字段
    public static final String QUERY = "innerId";//innerId,loginname
    public static final String QUERY1 = "outterId";//innerId,loginname
//  经常需要查询的字段  "identityCard";"_id";"innerId";
    //每次查询$in 数量
    public static final int SIZE= 1000;
    //导出字段
    public static final int ID = 0; //主键_id 0:不导出 1:导出
    public static final String RESULT = "ext.name_card.station";
//  经常需要导出的字段  "certificate_code";"jobNumber";"outterId";

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("10.127.6.126", 27017);
        try {
            //连接Mongo
            MongoDatabase database = mongoClient.getDatabase(DATABASE);
            MongoCollection<Document> collection = database.getCollection(COLLECTION);
            System.out.println(collection);
            //读取文件
            List<String> loginnames = readFile("D:\\abc.txt");
            int size = loginnames.size()%SIZE>0?loginnames.size()/SIZE  + 1 : loginnames.size()/SIZE;
            int index = 0;
//            List<Map<String, String>> maps = new ArrayList<>();
            CSVWriter csvWriter = new CSVWriter();
            System.out.println("一共多少次------"+size);
            String out = "D:/abc.csv";
            while (index < size) {
                long start = System.currentTimeMillis();
                if (index == size -1 ) {
                    List<String> selects = loginnames.subList(index*SIZE, loginnames.size());
                    csvWriter.writeAsCSV(select(selects, index, collection),out);
                }else {
                    List<String> selects = loginnames.subList(index*SIZE, index*SIZE+SIZE);
                    csvWriter.writeAsCSV(select(selects, index, collection),out);
                }

                System.out.println("第"+index+"次，耗时:" + ((System.currentTimeMillis() - start)/1000) + "秒");
                index++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }

    }

    private static List<Map<String, String>> select(List<String> loginnames,int index,MongoCollection<Document> collection) throws FileNotFoundException {

        JsonFlattener flattener = new JsonFlattener();
        //查询条件
        BasicDBList values = new BasicDBList();
        values.addAll(loginnames);
        BasicDBObject obj = new BasicDBObject(QUERY, new BasicDBObject("$in",values));//.append("is_delete",false);
//        BasicDBObject obj = new BasicDBObject(QUERY, new BasicDBObject("$in",values));
        //结果集字段
        BasicDBObject object = new BasicDBObject(QUERY, 1).append(QUERY1,1).append("_id",ID);
        //执行查找
        FindIterable<Document> documents = collection.find(obj).projection(object);
        StringBuffer s = new StringBuffer();
        s.append("[");
        for (Document document : documents) {
            s.append(document.toJson()+",");
        }
        if(s.indexOf(",")>1){
            s.delete(s.length()-1,s.length());
        }
        s.append("]");
        List<Map<String, String>> list = new ArrayList<>();
        try {
            list = flattener.handleAsArray(s.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }


    private static List<String> readFile(String filePath) throws IOException {
        List<String> list = FileUtils.readLines(new File(filePath), "UTF-8");
        return list;
    }

    private static Set<String> collectHeaders(List<Map<String, String>> flatJson) {
        Set<String> headers = new TreeSet<String>();
        for (Map<String, String> map : flatJson) {
            headers.addAll(map.keySet());
        }
        return headers;
    }
}
