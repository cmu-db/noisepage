import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.Executable;
import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import moglib.*;

/**
 * Test class that dynamically generate test cases for each sql query
 * Get file path from environment variable
 */
public class TracefileTest {
    private static File file;
    private static MogSqlite mog;
    private static Connection conn;
    private static final String OK = "ok";
    private static final String ERROR = "error";

    /**
     * Set up connection to database
     * Clear previous existing table
     * @throws Throwable
     */
    @BeforeEach
    public void setUp() throws Throwable {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        conn = TestUtility.makeDefaultConnection();
        String path = System.getenv("NOISEPAGE_TRACE_FILE");
        if (path == null || path.isEmpty()) {
            throw new RuntimeException("No 'trace-path' environment variable was specified");
        }
        System.out.println("File name: " + path);
        file = new File(path);
        mog = new MogSqlite(file);
    }



    /**
     * Factory method to generate test
     * @return a collection of DynamicTest object constructed from executables
     * @throws Throwable
     */
    @TestFactory
    public Collection<DynamicTest> generateTest() throws Throwable {
        Collection<DynamicTest> dTest = new ArrayList<>();
        int lineCounter = -1;
        // get all query start numbers
        List<Integer> queryLine = TestUtility.getQueryLineNum(file);

        // loop through every sql statement
        while (mog.next()) {
            // case for create and insert statements
            lineCounter++;
            if(mog.skip_status){
                mog.skip_status = false;
                continue;
            }
            String cur_sql = mog.sql.trim();
            int num = queryLine.get(lineCounter);
            if (mog.queryFirstLine.contains("statement")) {
                Statement statement = null;
                Executable exec = null;
                String testName = "Line:" + num + " | Expected " + mog.status;
                try {
                    statement = conn.createStatement();
                    statement.execute(cur_sql);
                    if(mog.status.equals(ERROR)){
                        String message = "Failure at Line " + num + ": Expected failure but success"  + "\n " + cur_sql;
                        exec = () -> check2(message);
                    }else{
                        exec = () -> assertEquals(true, true);
                    }
                }
                catch (Throwable e) {
                    if(mog.status.equals(OK)){
                        String message = "Failure at Line " + num + ": Expected success but failure"  + "\n " + cur_sql;
                        exec = () -> check2(message);
                    }else{
                        exec = () -> assertEquals(true, true);
                    }
                }
                DynamicTest cur = DynamicTest.dynamicTest(testName, exec);
                dTest.add(cur);
            } else{
                // case for query statements
                String parsed_hash;
                if(mog.queryResults.size()==0 || (!mog.queryResults.get(0).contains("values"))) {
                    parsed_hash = TestUtility.getHashFromDb(mog.queryResults);
                }else{
                    // parse the line from test file to get the hash
                    String[] sentence = mog.queryResults.get(0).split(" ");
                    parsed_hash = sentence[sentence.length - 1];
                }
                String testName = "Line:" + num +" | Hash:" + parsed_hash;
                // execute sql query to get result from database
                Statement statement;
                List<String> res;
                Executable exec;
                try {
                    statement = conn.createStatement();
                    statement.execute(cur_sql);
                    ResultSet rs = statement.getResultSet();
                    res = mog.processResults(rs);
                    // create an executable for the query
                    String hash2 = TestUtility.getHashFromDb(res);
                    exec = () -> check(parsed_hash, hash2, num, cur_sql);
                    // create the DynamicTest object
                } catch (Throwable e) {
                    String message = "Failure at Line " + num + ": " + e.getMessage() + "\n" + cur_sql;
                    exec = () -> check2(message);
                }
                DynamicTest cur = DynamicTest.dynamicTest(testName, exec);
                dTest.add(cur);
            }
            mog.queryResults.clear();
        }
        conn.close();
        return dTest;

    }

    /**
     * compare hash, print line number and error if hash don't match
     * @param hash1 hash
     * @param hash2 hash
     * @param n line number
     * @throws Exception
     */
    public static void check(String hash1, String hash2, int n, String sql) throws Exception {
        try {
            assertEquals(hash1, hash2);
        }
        catch (AssertionError e) {
            throw new Exception("Failure at Line " + n + ": " + e.getMessage() + "\n" + sql);
        }
    }

    /**
     * wrapper for throwing message for the case that non-select
     * sql statements fail
     * @param mes message to print out
     * @throws Exception
     */
    public static void check2(String mes) throws Exception {
        throw new Exception(mes);
    }

    public static void removeExistingTable(List<String> tab, Connection connection) throws SQLException {
        for(String i:tab){
            Statement st = connection.createStatement();
            String sql = "DROP TABLE IF EXISTS " + i + " CASCADE";
            st.execute(sql);
        }
    }
    public static List<String> getAllExistingTableName(MogSqlite mog,Connection connection) throws SQLException {
        Statement st = connection.createStatement();
        String getTableName = "SELECT tablename FROM pg_tables WHERE schemaname = 'public';";
        st.execute(getTableName);
        ResultSet rs = st.getResultSet();
        List<String> res = mog.processResults(rs);
        return res;
    }

}
