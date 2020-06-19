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
public class dynamicTest2 {
//    private File file;
//    private MogSqlite mog;
//    private static final String URL = "jdbc:postgresql://localhost/jeffdb";
//    private static final String USER = "jeffniu";
//    private static final String PASSWORD = "";
//    private MogDb db;
//    public dynamicTest(String path) throws FileNotFoundException {
//        this.file = new File(path);
//        this.mog = new MogSqlite(this.file);
//        this.db = new MogDb("dbTestJdbc", "dbTestUser", "dbTestPass");
//    }
//    @BeforeEach
//    public void setUp(){
//        file = new File("script/testing/junit/src/select1.test");
//        mog = new MogSqlite(file);
//    }
    public static void main(String[] args) throws Throwable {
//        dynamicTest cur = new dynamicTest(args[0]);
//        cur.setUp();
//        Collection<DynamicTest> dtest = cur.curTest();
//        for(DynamicTest e:dtest){
//            e.getExecutable().execute();
//        }
//
//        System.out.println(dtest);
//        System.out.println("hii");
        String path = args[0];
        File file = new File(path);
        MogSqlite mog = new MogSqlite(file);
        String url = "jdbc:postgresql://localhost/jeffdb";
        String user = "jeffniu";
        String password = "";
        MogDb db = new MogDb(url, user, password);
        Connection conn = db.getDbTest().newConn();
//        System.out.println(conn);
//        System.out.println("lol");
//        System.out.println(MogUtil.bytesToHex("358364".getBytes()));
        int counter = 0;
        int lineCounter = -1;
        List<Integer> queryLine = getQueryLineNum(file);
//        System.out.println(queryLine);

        List<String> tab = getAllExistingTableName(mog,db);
        removeExistingTable(tab,db);
//        while(rs.next()){
//            System.out.println("sdaasdasd   "+ rs.next());
//        }

        while (mog.next()) {
//            System.out.println(mog.lineNum);
//            System.out.println(mog.lineCounter);
//            System.out.println(mog.sql);

//            Executable exec = ()
            // mog.queryResults: first [] for insert, then query result
//            System.out.println(mog.queryResults);
            // insert values into database
            if (mog.queryResults.size() == 0) {
                Statement statement = db.getDbTest().getConn().createStatement();
                statement.execute(mog.sql);
            } else{
                System.out.println(mog.queryResults);
                if(mog.queryResults.get(0).contains("values")){
                    lineCounter++;
                    // parse the hash
                    String[] sentence = mog.queryResults.get(0).split(" ");
                    String hash = sentence[sentence.length-1];
//                    System.out.println(queryLine.get(lineCounter));
//                    System.out.println(mog.sql);
//                    System.out.println(mog.lineNum);
//                    System.out.println(mog.lineCounter);
//                    System.out.println("enternow");
//                    System.out.println(hash);
//                    System.out.println(queryHashFromDb(mog,db));

                    assertEquals(hash, queryHashFromDb(mog,db));
                    if(counter>5){
                        break;
                    }
                    counter++;
//                    assertEquals(hash,queryHashFromDb(mog,db));
                }
            }
        }
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
        System.out.println("sdaasdasd   "+ rs.next());
        System.out.println("sdaasdasd   "+ res);
        return res;
    }
}
