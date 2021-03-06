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
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 依user表查询MSS表
 */
public class MssExport {
    public static final int SIZE = 100;

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("10.127.6.126", 27017);

        try {
            List<String> loginnames = readFile("D:\\abc.txt");
            String outfile = "D:\\abcd.csv";
            int size = loginnames.size() % SIZE > 0 ? loginnames.size() / SIZE + 1 : loginnames.size() / SIZE;
            int index = 0;
            System.out.println("需要多少次 = " + size);
            CSVWriter writer = new CSVWriter();
            while (index < size) {
                long start = System.currentTimeMillis();
                if (index == size - 1) {
                    List<String> selects = loginnames.subList(index * SIZE, loginnames.size());
                    List<Map<String, String>> maps = selects(selects, mongoClient);
                    writer.writeAsCSV(maps, outfile);
                } else {
                    List<String> selects = loginnames.subList(index * SIZE, index * SIZE + SIZE);
                    List<Map<String, String>> maps = selects(selects, mongoClient);
                    writer.writeAsCSV(maps, outfile);
                }
                System.out.println("第" + index + "次，耗时:" + ((System.currentTimeMillis() - start) / 1000) + "秒");
                index++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }
    }

    private static List<Map<String, String>> selects(List<String> selects, MongoClient mongoClient) {
        try {
            MongoDatabase database = mongoClient.getDatabase("repository_d_telecom");
            MongoCollection<Document> collection = database.getCollection("user");
            JsonFlattener flattener = new JsonFlattener();
            BasicDBList values = new BasicDBList();
            values.addAll(selects);
            System.out.println(values.size());
            System.out.println("开始查询");
            BasicDBObject parobj = new BasicDBObject("loginname", new BasicDBObject("$in", values)).append("is_delete", false);
            BasicDBObject resultobj = new BasicDBObject("loginname", 1).append("certificate_code", 1).append("ext.name_card.mobile", 1).append("_id", 0);
            FindIterable<Document> documents = collection.find(parobj).projection(resultobj);
            StringBuffer s = new StringBuffer();
            s.append("[");
            for (Document document : documents) {
                s.append(document.toJson() + ",");
            }
            if (s.indexOf(",") > 1) {
                s.delete(s.length() - 1, s.length());
            }
            s.append("]");
            List<Map<String, String>> list = flattener.handleAsArray(s.toString());
            database = mongoClient.getDatabase("mss_sync_db");
            collection = database.getCollection("mSSHRInfomation");
            for (int i = 0; i < list.size(); i++) {
                Map<String, String> map = list.get(i);
                String code = map.get("certificate_code");
                FindIterable<Document> iterable = collection.find(new BasicDBObject("identityCard", code)).projection(new BasicDBObject("identityCard", 1).append("jobNumber", 1).append("_id", 0));
                for (Document document : iterable) {
                    map.put("jobnumber", (String) document.get("jobNumber"));
                }
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static List<String> readFile(String filePath) throws IOException {
        List<String> list = FileUtils.readLines(new File(filePath), "UTF-8");
        return list;
    }
}
