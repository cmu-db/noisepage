import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.Executable;
import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
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

    /**
     * Set up connection to database
     * Clear previous existing table
     *
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
        List<Integer> queryLine = getQueryLineNum(file);

        // loop through every sql statement
        while (mog.next()) {
            // case for create and insert statements
            lineCounter++;
            String cur_sql = mog.sql.trim();
            int num = queryLine.get(lineCounter);
            if (mog.queryFirstLine.equals(Constants.STATEMENT_ERROR) || mog.queryFirstLine.equals(Constants.STATEMENT_OK)) {
                DynamicTest cur = executeNonSelectQuery(num, cur_sql);
                dTest.add(cur);
            } else {
                // case for query statements
                DynamicTest cur = executeSelectQuery(num, cur_sql);
                dTest.add(cur);
            }
            mog.queryResults.clear();
        }
        conn.close();
        return dTest;
    }

    /**
     * execute select queries
     * @param num line number
     * @param cur_sql sql statement
     * @return DynamicTest obj
     */
    public static DynamicTest executeSelectQuery(int num, String cur_sql) {
        String parsed_hash;
        int parsed_num_result = 0;
        boolean check_expected = false;
        if (mog.queryResults.size() == 0 || (!mog.queryResults.get(0).contains(Constants.VALUES))) {
            parsed_hash = TestUtility.getHashFromDb(mog.queryResults);
        } else {
            // parse the line from test file to get the hash
            String[] sentence = mog.queryResults.get(0).split(" ");
            parsed_hash = sentence[sentence.length - 1];
            try {
                parsed_num_result = Integer.parseInt(sentence[0]);
            } catch (Exception e) {
                parsed_num_result = Integer.parseInt(sentence[1]);
                check_expected = true;
            }
        }
        String testName = "Line:" + num + " | Hash:" + parsed_hash;
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
            String message = "Failure at Line " + num + ": " + "\n" + cur_sql + "\n" +
                    "Query expected " + parsed_num_result + " results, got " + res.size() + " results"
                    + "\n" + res;
            boolean len_match = getCheckLength(check_expected, parsed_num_result, res.size());
            exec = () -> checkResultMatch(parsed_hash, hash2, message, len_match, res);
        } catch (Throwable e) {
            String message = "Failure at Line " + num + ": " + e.getMessage() + "\n" + cur_sql;
            exec = () -> checkAlwaysFail(message);
        }
        DynamicTest cur = DynamicTest.dynamicTest(testName, exec);
        return cur;
    }

    public static boolean getCheckLength(boolean check_expected, int parsed_num_result, int actual_result){
        boolean len_match;
        if (check_expected) {
            if (parsed_num_result == actual_result) {
                len_match = true;
            } else {
                len_match = false;
            }
        } else {
            len_match = true;
        }
        return len_match;
    }

    /**
     * execute non-select queries
     * @param num line number
     * @param cur_sql sql statement
     * @return DynamicTest obj
     */
    public static DynamicTest executeNonSelectQuery(int num, String cur_sql){
        String testName = "Line:" + num + " | Expected " + mog.queryFirstLine;
        Statement statement = null;
        Executable exec = null;
        try {
            statement = conn.createStatement();
            statement.execute(cur_sql);
            if(mog.queryFirstLine.equals(Constants.STATEMENT_ERROR)){
                String message = "Failure at Line " + num + ": Expected failure but success"  + "\n " + cur_sql;
                exec = () -> checkAlwaysFail(message);
            }else{
                exec = () -> assertEquals(true, true);
            }
        }
        catch (Throwable e) {
            if(mog.queryFirstLine.equals(Constants.STATEMENT_OK)){
                String message = "Failure at Line " + num + ": Expected success but failure"  + "\n " + cur_sql;
                exec = () -> checkAlwaysFail(message);
            }else{
                exec = () -> assertEquals(true, true);
            }
        }
        DynamicTest cur = DynamicTest.dynamicTest(testName, exec);
        return cur;
    }
    /**
     * compare hash, print line number and error if hash don't match
     * @param hash1 hash
     * @param hash2 hash
     * @param message error message
     * @param len_match boolean that indicate if the number of value queried is correct
     * @throws Exception
     */
    public static void checkResultMatch(String hash1, String hash2, String message,
                             boolean len_match, List<String> res) throws Exception {
        if(!len_match){
            throw new Exception("Query got wrong number of values");
        }
        try {
            assertEquals(hash1, hash2);
        }
        catch (AssertionError e) {
            if(len_match){
                List<String> new_res = new ArrayList<>();
                for(String i:res){
                    try{
                        int cur = (int)Double.parseDouble(i);
                        new_res.add(cur+"");
                    }catch(Exception e1){
                        new_res.add(i);
                    }
                }
                String new_hash = TestUtility.getHashFromDb(new_res);
                assertEquals(hash1, new_hash);
            }else{
                throw new Exception(message + "\n" + e.getMessage());
            }
        }
    }

    /**
     * wrapper for throwing message for the case that sql statements fail
     * @param mes message to print out
     * @throws Exception
     */
    public static void checkAlwaysFail(String mes) throws Exception {
        throw new Exception(mes);
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
            if(line.startsWith(Constants.QUERY) || line.startsWith(Constants.STATEMENT_ERROR)
            || line.startsWith(Constants.STATEMENT_OK)){
                res.add(counter);
            }
        }
        return res;
    }
}