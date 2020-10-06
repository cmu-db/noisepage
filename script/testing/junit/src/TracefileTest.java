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
import static org.junit.jupiter.api.Assertions.assertThrows;

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
            if (mog.queryFirstLine.contains(Constants.STATEMENT_ERROR) || mog.queryFirstLine.contains(Constants.STATEMENT_OK)) {
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
        // if check_expected is true, we check if length of the result lists match
        boolean check_expected = false;
        // if onlyResult is true, we only compare the whole result lists instead of hash
        boolean onlyResult = false;
        // if there's no query result expected, or whole result list is shown
        // instead of hash, set onlyResult to true
        if (mog.queryResults.size() == 0 || (!mog.queryResults.get(0).contains(Constants.VALUES))) {
            parsed_hash = TestUtility.getHashFromDb(mog.queryResults);
            onlyResult = true;
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
            if(onlyResult){
                List<String> temp = new ArrayList<>(mog.queryResults);
                exec = () -> checkEquals(res,temp);
            }else{
                String hash2 = TestUtility.getHashFromDb(res);
                String message = "Failure at Line " + num + ": " + "\n" + cur_sql + "\n" +
                        "Query expected " + parsed_num_result + " results, got " + res.size() + " results"
                        + "\n" + res+"\n"+mog.queryResults;
                // len_match, boolean to indicate if result length match
                boolean len_match = getCheckLength(check_expected, parsed_num_result, res.size());
                exec = () -> checkResultMatch(parsed_hash, hash2, message, len_match, res);
            }
        } catch (Throwable e) {
            String message = "Failure at Line " + num + ": " + e.getMessage() + "\n" + cur_sql;
            exec = () -> checkAlwaysFail(message);
        }
        DynamicTest cur = DynamicTest.dynamicTest(testName, exec);
        return cur;
    }

    public static void checkEquals(List<String> res, List<String> queryResult) throws Exception {
        if(res.size()!=queryResult.size()){
            throw new Exception("Query got wrong number of results");
        }
        double precision = 0.000001;
        for(int i=0;i<res.size();i++){
            if(res.get(i)==null){
                if(queryResult.get(i)!=null){
                    throw new Exception("Value null Mismatch");
                }
            }else if(res.get(i).equals("")){
                if(!queryResult.get(i).equals("")){
                    throw new Exception("Value '' Mismatch");
                }
            }else{
                try{
                    double one = Double.parseDouble(res.get(i));
                    double two = Double.parseDouble(queryResult.get(i));
                    if(Math.abs(one-two)>precision){
                        throw new Exception("Expected " + queryResult + " but have " + res);
                    }
                }catch(Exception e){
                    if(!res.get(i).equals(queryResult.get(i))){
                        throw new Exception("Expected " + queryResult + " but have " + res);
                    }
                }
            }
        }
    }

    /**
     * check if length of queried result equals length of expected result
     * @param check_expected flag: whether to check length or not
     * @param parsed_num_result parsed result length
     * @param actual_result actual length
     * @return true if length match or aren't required to be checked
     */
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
            if(mog.queryFirstLine.contains(Constants.STATEMENT_ERROR)){
                String message = "Failure at Line " + num + ": Expected failure but success"  + "\n " + cur_sql;
                exec = () -> checkAlwaysFail(message);
            }else{
                exec = () -> assertEquals(true, true);
            }
        }
        catch (SQLException e) {
            String code = e.getSQLState();
            if(mog.queryFirstLine.contains(Constants.STATEMENT_OK)){
                String message = "Failure at Line " + num + ": Expected success but failure"  + "\n " + cur_sql;
                exec = () -> checkAlwaysFail(message);
            }else{
                // statement error case, with optional error code flag
                String[] arr = mog.queryFirstLine.split(" ");
                String parsed_code = arr[arr.length-1];
                int code2 = -1;
                try{
                    code2 = Integer.parseInt(parsed_code);
                    if(Integer.parseInt(code)!=code2){
                        exec = () -> checkAlwaysFail("Error code mismatch");
                    }else{
                        exec = () -> assertEquals(true, true);
                    }
                }catch(Exception e1){
                    // no error code specified case, just return true
                    exec = () -> assertEquals(true, true);
                }
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
        // if length doesn't match, throw exception
        if(!len_match){
            throw new Exception("Query got wrong number of values");
        }
        // try comparing the hash
        try {
            assertEquals(hash1, hash2);
        }
        catch (AssertionError e) {
            // if hash doesn't match
            if(len_match){
                // if length still match, cast the double to int to compare again (dealing with float
                // precision errors
                List<String> new_res = new ArrayList<>();
                for(String i:res){
                    try{
                        int cur = (int)Math.round(Double.parseDouble(i));
                        new_res.add(cur+"");
                    }catch(Exception e1){
                        new_res.add(i);
                    }
                }
                String new_hash = TestUtility.getHashFromDb(new_res);
                try{
                    assertEquals(hash1, new_hash);
                }catch (AssertionError e1){
                    throw new Exception(message+"\n"+e.getMessage()+res);
                }
            }else{
                // if length doesn't match, throw exception
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