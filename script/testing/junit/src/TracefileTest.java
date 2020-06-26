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
 * Test class that dynamically generate test cases for each sql query
 * Specify file path in environment
 */
public class TracefileTest {
    // cmake ..;  build terrier
    private File file;
    private MogSqlite mog;
    private static final String URL = "jdbc:postgresql://localhost/jeffdb";
    private static final String USER = "jeffniu";
//    private static final String URL = "jdbc:postgresql://localhost:15721/";
//    private static final String USER = "terrier";
    private static final String PASSWORD = "";
    private MogDb db;
    private static Connection conn;

    /**
     * Set up connection to database
     * Clear previous existing table
     * @throws FileNotFoundException
     * @throws SQLException
     */
    @BeforeEach
    public void setUp() throws FileNotFoundException, SQLException {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        System.setProperty("testFile", "src/select1.test");
        String path = System.getProperty("testFile");
        file = new File(path);
        mog = new MogSqlite(file);
        db = new MogDb(URL, USER, PASSWORD);
//        conn = db.getDbTest().newConn();
        conn = TestUtility.makeDefaultConnection();
        System.out.println(conn);
        Statement statement = conn.createStatement();
        List<String> tab = getAllExistingTableName(mog,db);
        removeExistingTable(tab,db);
    }

    // TODO: make the input path environment
    // TODO: try making a tracefile by hand, take SELECT.JAVA and convert it to select1.test

    /**
     * Factory method to generate test
     * @return a collection of DynamicTest object constructed from executables
     * @throws IOException
     * @throws SQLException
     */
    @TestFactory
    public Collection<DynamicTest> generateTest() throws IOException, SQLException {
        Collection<DynamicTest> dTest = new ArrayList<>();
        int lineCounter = -1;
        // get all query start numbers
        List<Integer> queryLine = getQueryLineNum(file);
        // loop through every sql statement
        while (mog.next()) {
            System.out.println(mog.sql);
            // case for create and insert statements
            if (mog.queryResults.size() == 0) {
                Statement statement = conn.createStatement();
                System.out.println(mog.sql);
                statement.execute(mog.sql);
                System.out.println("1");
                System.out.println(conn);
            } else{
                // case for query statements
                System.out.println(conn);
                if(mog.queryResults.get(0).contains("values")){
                    lineCounter++;
                    // parse the line from test file to get the hash
                    String[] sentence = mog.queryResults.get(0).split(" ");
                    String hash = sentence[sentence.length-1];
                    // execute sql query to get result from database
                    Statement statement = conn.createStatement();
                    System.out.println(mog.sql);
                    System.out.println(conn);
                    statement.execute(mog.sql);
                    System.out.println("2");
                    ResultSet rs = statement.getResultSet();
                    List<String> res = mog.processResults(rs);
                    // create an executable for the query
                    Executable exec = () -> assertEquals(getHashFromDb(res), hash);
                    String testName = "Line: " + queryLine.get(lineCounter)+"|Hash: "+hash;
                    // create the DynamicTest object
                    DynamicTest cur = DynamicTest.dynamicTest(testName, exec);
                    dTest.add(cur);
                }
            }
        }
        return dTest;

    }

    /**
     * Compute the hash from result list
     * @param res result list of strings queried from database
     * @return hash computed
     */
    public static String getHashFromDb(List<String> res)  {
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

    /**
     * Get all sql query statement start line numbers
     * @param input test file
     * @return list of integers that contains start line numbers
     * @throws IOException
     */
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

    /**
     * Remove existing table from database
     * @param tab list of strings containing existing table names
     * @param db testing database
     * @throws SQLException
     */
    public static void removeExistingTable(List<String> tab, MogDb db) throws SQLException {
        for(String i:tab){
            Statement st = conn.createStatement();
            String sql = "DROP TABLE IF EXISTS " + i + " CASCADE";
            st.execute(sql);
        }
    }

    /**
     * Get existing table names
     * @param mog MogSqlite obj
     * @param db testing database
     * @return list of strings containing existing table names
     * @throws SQLException
     */
    public static List<String> getAllExistingTableName(MogSqlite mog,MogDb db) throws SQLException {
        Statement st = conn.createStatement();
        String getTableName = "SELECT tablename FROM pg_tables WHERE schemaname = 'public';";
        String terrier_table = "SELECT relname FROM pg_class WHERE relkind = 114 AND relnamespace = 15;";
//        st.execute(getTableName);
        st.execute(terrier_table);
        ResultSet rs = st.getResultSet();
        List<String> res = mog.processResults(rs);
        System.out.println("Current table   "+ res);
        return res;
    }
}
