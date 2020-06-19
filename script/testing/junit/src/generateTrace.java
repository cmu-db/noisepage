import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;

import java.io.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * one class file dynamically generate test cases for each query
 * provide
 */
public class generateTrace {
    private static final String STATEMENT_OK = "statement ok";
    public static final String QUERY_I_NOSORT = "query I nosort";
    public static final String SEPARATION = "----";
    public static void main(String[] args) throws Throwable {
        String path = args[0];
        File file = new File(path);
        MogSqlite mog = new MogSqlite(file);
        String url = "jdbc:postgresql://localhost/jeffdb";
        String user = "jeffniu";
        String password = "";
        MogDb db = new MogDb(url, user, password);
        Connection conn = db.getDbTest().newConn();
        List<String> tab = getAllExistingTableName(mog,db);
        removeExistingTable(tab,db);

        String line;
        BufferedReader br = new BufferedReader(new FileReader(file));
        FileWriter writer = new FileWriter(new File("script/testing/junit/src","output.test"));
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        while (null != (line = br.readLine())) {
            line = line.trim();
            System.out.println(line);
            Statement statement = conn.createStatement();
            statement.execute(line);
            if(line.startsWith("SELECT")){
                writeToFile(writer, QUERY_I_NOSORT);
                writeToFile(writer, line);
                writeToFile(writer, SEPARATION);
                ResultSet rs = statement.getResultSet();
                List<String> res = mog.processResults(rs);
                MessageDigest md;
                try {
                    md = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("no Alg", e);
                }
                String resultString = String.join("\n", res) + "\n";
                md.update(resultString.getBytes());
                byte[] byteArr = md.digest();
                String hash = MogUtil.bytesToHex(byteArr).toLowerCase();
                int num = res.size();
                String queryResult = num + " values hashing to " + hash;
                writeToFile(writer, queryResult);
                writer.write('\n');
            }else{
                writeToFile(writer, STATEMENT_OK);
                writeToFile(writer, line);
                System.out.println("hii "+line);
                writer.write('\n');
            }
        }
        writer.close();

//        while (mog.next()) {
//            System.out.println(mog.queryResults);
//            System.out.println(mog.sql);
//            Statement statement = db.getDbTest().getConn().createStatement();
//            statement.execute(mog.sql);
//            if(mog.sql.startsWith("SELECT")){
//                System.out.println("hi");
//                ResultSet rs = statement.getResultSet();
//                // res contains a list of results
//                List<String> res = mog.processResults(rs);
//                System.out.println(res);
//                System.out.println("hi2");
//                }

//            if (mog.queryResults.size() == 0) {
////                Statement statement = db.getDbTest().getConn().createStatement();
////                statement.execute(mog.sql);
//                Statement statement = db.getDbTest().getConn().createStatement();
//                statement.execute(mog.sql);
//                if(mog.sql.startsWith("SELECT")){
//                    System.out.println("hi");
//                    ResultSet rs = statement.getResultSet();
//                    // res contains a list of results
//                    List<String> res = mog.processResults(rs);
//                    System.out.println(res);
//
//                    System.out.println("hi");
//                }
//            } else{
//                System.out.println("enter");
//                System.out.println(mog.queryResults);
//                if(mog.queryResults.get(0).contains("values")){
//                    lineCounter++;
//                    // parse the hash
//                    String[] sentence = mog.queryResults.get(0).split(" ");
//                    String hash = sentence[sentence.length-1];
////                    System.out.println(queryLine.get(lineCounter));
////                    System.out.println(mog.sql);
////                    System.out.println(mog.lineNum);
////                    System.out.println(mog.lineCounter);
////                    System.out.println("enternow");
//
////                    System.out.println(hash);
////                    System.out.println(queryHashFromDb(mog,db));
//
//                    assertEquals(hash, queryHashFromDb(mog,db));
//                    if(counter>5){
//                        break;
//                    }
//                    counter++;
////                    assertEquals(hash,queryHashFromDb(mog,db));
////                }
//            }
//        }
    }
    public static void writeToFile(FileWriter writer, String str) throws IOException {
        writer.write(str);
        writer.write('\n');
    }
    public static String queryHashFromDb(MogSqlite mog, MogDb db) throws SQLException, IOException {
        Statement statement = db.getDbTest().getConn().createStatement();
        statement.execute(mog.sql);
        ResultSet rs = statement.getResultSet();
        // res contains a list of results
        List<String> res = mog.processResults(rs);
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("no Alg", e);
        }
        String resultString = String.join("\n", res) + "\n";
        md.update(resultString.getBytes());
        byte[] byteArr = md.digest();
        String hex = MogUtil.bytesToHex(byteArr);
        return hex.toLowerCase();
    }
    public static List<Integer> getQueryLineNum(File input) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(input));
        List<Integer> res = new ArrayList<>();
        String line;
        int counter = 0;
        while (null != (line = bf.readLine())){
            counter++;
            if(line.startsWith("query")){
                res.add(counter);
            }
        }
        return res;
    }
    public static void removeExistingTable(List<String> tab, MogDb db) throws SQLException {
        for(String i:tab){
            Statement st = db.getDbTest().getConn().createStatement();
            String sql = "DROP TABLE IF EXISTS " + i + " CASCADE";
            st.execute(sql);
        }
    }
    public static List<String> getAllExistingTableName(MogSqlite mog,MogDb db) throws SQLException {
        Statement st = db.getDbTest().getConn().createStatement();
        String getTableName = "SELECT tablename FROM pg_tables WHERE schemaname = 'public';";
        st.execute(getTableName);
        ResultSet rs = st.getResultSet();
        List<String> res = mog.processResults(rs);
//        System.out.println("sdaasdasd   "+ rs.next());
//        System.out.println("sdaasdasd   "+ res);
        return res;
    }
}
