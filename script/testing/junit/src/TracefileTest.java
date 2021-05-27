import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.Executable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import moglib.*;

/**
 * The Logger class implements a dummy logger that,
 * for the time being, just prints to standard output.
 */
final class Logger {
    /**
     * Construct a new Logger instance.
     */
    public Logger() {

    }

    /**
     * Log a standard mesasge.
     * @param message The message
     */
    public void log(final String message) {
        System.out.println(message);
    }

    /**
     * Log an error message.
     * @param message The message
     */
    public void error(final String message) {
        System.err.println(message);
    }
}

/**
 * The TracefileTest class implements automated integration testing
 * via tracefiles. The class encapsulates all of the logic necessary to:
 *  - Read and parse tracefile contents
 *  - Execute the specified queries against the NoisePage DBMS server
 *  - Compare the results with a reference output
 *  - Report errors when necessary
 */
public class TracefileTest {
    // ------------------------------------------------------------------------
    // Static Members
    // ------------------------------------------------------------------------

    /**
     * The input tracefile.
     */
    private static File file;

    /**
     * The mog instance.
     */
    private static MogSqlite mog;

    /**
     * The DBMS server connection.
     */
    private static Connection conn;

    /**
     * The logger instance.
     */
    private static final Logger logger = new Logger();

    // ------------------------------------------------------------------------
    // Test Setup
    // ------------------------------------------------------------------------

    /**
     * Perform per-test setup.
     * @throws Throwable
     */
    @BeforeEach
    public void setUp() throws Throwable {
        logger.log("Working Directory = " + System.getProperty("user.dir"));
        
        conn = TestUtility.makeDefaultConnection();
        final String path = System.getenv("NOISEPAGE_TRACE_FILE");
        if (path == null || path.isEmpty()) {
            throw new RuntimeException("No 'trace-path' environment variable was specified");
        }
        
        logger.log("File Name: " + path);

        file = new File(path);
        mog = new MogSqlite(file);
    }

    // ------------------------------------------------------------------------
    // Test Generation
    // ------------------------------------------------------------------------

    /**
     * Factory method to generate a test case for a tracefile.
     * @return a collection of DynamicTest object constructed from executables.
     * @throws Throwable
     */
    @TestFactory
    public Collection<DynamicTest> generateTest() throws Throwable {
        // We populate this collection with a DynamicTest instance
        // for each query we encounter in the current tracefile
        List<DynamicTest> dynamicTests = new ArrayList<>();
        
        // Get all query start numbers
        final List<Integer> queryLineNumbers = getQueryLineNumbers(file);

        // Iterate until all queries from tracefile are exhausted
        int lineCounter = 0;
        while (mog.next()) {
            // Grab the current SQL query from the mog instance
            final String currentSQL = mog.sql.trim();

            // Get the line number that corresponds to the current counter value
            final int lineNumber = queryLineNumbers.get(lineCounter);

            if (mog.queryFirstLine.contains(Constants.STATEMENT_ERROR)
             || mog.queryFirstLine.contains(Constants.STATEMENT_OK)) {
                dynamicTests.add(executeNonSelectQuery(lineNumber, currentSQL));
            } else {
                dynamicTests.add(executeSelectQuery(lineNumber, currentSQL));
            }
            mog.queryResults.clear();
            if (conn.isClosed()) {
                logger.error("Connection closed unexpectedly, skipping remaining tests.");
                break;
            }

            lineCounter++;
        }
        conn.close();
        return dynamicTests;
    }

    // ------------------------------------------------------------------------
    // Test Execution
    // ------------------------------------------------------------------------

    /**
     * Execute a SELECT query.
     * @param lineNumber The line number for the start of the query
     * @param queryString The raw query string
     * @return A DynamicTest instance that represents the test case
     */
    private static DynamicTest executeSelectQuery(final int lineNumber, final String queryString) {        
        // Extract the configuration for the current query
        final String expectedHash = getParsedHash();
        final boolean onlyResult = getCheckOnlyResult();
        final int expectedResultCount = getParsedResultCount();
        final boolean checkExpectedLength = getCheckExpectedLength();

        // execute sql query to get result from database
        Executable exec;
        try {
            // Execute the query
            Statement statement = conn.createStatement();
            statement.execute(queryString);
            
           // Process the result set from the query
            final List<String> results = mog.processResults(statement.getResultSet());
            
            // Create an executable for the query
            if (onlyResult) {
                List<String> temp = new ArrayList<>(mog.queryResults);
                exec = () -> checkEquals(results, temp);
            } else {
                final String resultHash = TestUtility.getHashFromDb(results);

                StringBuilder builder = new StringBuilder();
                builder.append("Failure at Line ");
                builder.append(lineNumber);
                builder.append(": \n");
                builder.append(queryString);
                builder.append("\nQuery expected ");
                builder.append(expectedResultCount);
                builder.append("results, got");
                builder.append(results.size());
                builder.append(" results\n");
                builder.append(results);
                builder.append('\n');
                builder.append(mog.queryResults);

                final String message = builder.toString();

                final boolean lengthsMatch = getCheckLength(checkExpectedLength, expectedResultCount, results.size());
                exec = () -> checkResultMatch(expectedHash, resultHash, message, lengthsMatch, results);
            }
        } catch (Throwable e) {
            final String message = buildTestFailureString(lineNumber, queryString, e.getMessage());
            exec = () -> checkAlwaysFail(message);
        }

        return DynamicTest.dynamicTest(buildTestNameString(lineNumber, queryString, expectedHash), exec);
    }

    /**
     * Execute a non-SELECT query.
     * @param lineNumber The line number for the start of the query
     * @param queryString The raw query string
     * @return A DynamicTest instance that represents the test case
     */
    private static DynamicTest executeNonSelectQuery(final int lineNumber, final String queryString) {
        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append("Line: ");
        nameBuilder.append(lineNumber);
        nameBuilder.append(" | Expected ");
        nameBuilder.append(mog.queryFirstLine);
        final String testName = nameBuilder.toString();

        Executable exec;
        try {
            Statement statement = conn.createStatement();
            statement.execute(queryString);
            if (mog.queryFirstLine.contains(Constants.STATEMENT_ERROR)) {
                StringBuilder messageBuilder = new StringBuilder();
                messageBuilder.append("Failure at line ");
                messageBuilder.append(lineNumber);
                messageBuilder.append(": Expected failure but success\n ");
                messageBuilder.append(queryString);
                final String message = messageBuilder.toString();
                exec = () -> checkAlwaysFail(message);
            } else {
                // A no-op assertion
                exec = () -> assertTrue(true);
            }
        } catch (SQLException e) {
            final String resultCode = e.getSQLState();
            if (mog.queryFirstLine.contains(Constants.STATEMENT_OK)) {
                StringBuilder messageBuilder = new StringBuilder();
                messageBuilder.append("Failure at line ");
                messageBuilder.append(lineNumber);
                messageBuilder.append(": Expected success but failure\n ");
                messageBuilder.append(queryString);
                final String message = messageBuilder.toString();
                exec = () -> checkAlwaysFail(message);
            } else {
                // statement error case, with optional error code flag
                final String[] arr = mog.queryFirstLine.split(" ");
                try {
                    // TODO(Kyle): Is this array access always in bounds?
                    final int expectedCode = Integer.parseInt(arr[arr.length - 1]);
                    if (Integer.parseInt(resultCode) != expectedCode) {
                        exec = () -> checkAlwaysFail("Error code mismatch");
                    } else {
                        exec = () -> assertTrue(true);
                    }
                } catch (Exception ex) {
                    exec = () -> assertTrue(true);
                }
            }
        }
        return DynamicTest.dynamicTest(testName, exec);
    }

    // ------------------------------------------------------------------------
    // Correctness Checks
    // ------------------------------------------------------------------------

    /**
     * 
     * @param res
     * @param queryResult
     * @throws Exception
     */
    private static void checkEquals(List<String> res, List<String> queryResult) throws Exception {
        if(res.size()!=queryResult.size()){
            throw new Exception("Query got wrong number of results");
        }
        double precision = 0.00001;
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
     * Wrapper for throwing message for the case in which SQL statements fail.
     * @param message The error message
     * @throws RuntimeException
     */
    public static void checkAlwaysFail(final String message) throws RuntimeException {
        throw new RuntimeException(message);
    }

    /**
     * Get all sql query statement start line numbers.
     * @param input The input tracefile
     * @return A list of integers that contains start line numbers
     * @throws IOException
     */
    public static List<Integer> getQueryLineNumbers(File input) throws IOException {
        try (BufferedReader bf = new BufferedReader(new FileReader(input))) {
            List<Integer> res = new ArrayList<>();
            String line;
            int counter = 0;
            while (null != (line = bf.readLine())){
                counter++;
                if (line.startsWith(Constants.QUERY)
                || line.startsWith(Constants.STATEMENT_ERROR)
                || line.startsWith(Constants.STATEMENT_OK)) {
                    res.add(counter);
                }
            }
            return res;
        }
    }

    // ------------------------------------------------------------------------
    // Misc. Utilities
    // ------------------------------------------------------------------------

    /**
     * Determine if we should only check the expected length of the result sets.
     * @return `true` if the length of the result set should be checked, `false` otherwise
     */
    private static boolean getCheckExpectedLength() {
        // TODO(Kyle): I just ripped this logic out of the above
        // function, but this really does not make much sense to me
        final String[] sentence = mog.queryResults.get(0).split(" ");
        try {
            Integer.parseInt(sentence[0]);
        } catch (Exception e) {
            return true;
        }
        return false;
    }

    /**
     * Determine if we should check the result of the query, rather than the hash.
     * @return `true` if we check the result, `false` otherwise
     */
    private static boolean getCheckOnlyResult() {
        return (mog.queryResults.size() == 0 || (!mog.queryResults.get(0).contains(Constants.VALUES)));
    }

    /**
     * Get the parsed hash from the current query.
     * @return The parsed hash
     */
    private static String getParsedHash() {
        if (mog.queryResults.size() == 0 || (!mog.queryResults.get(0).contains(Constants.VALUES)))) {
            return TestUtility.getHashFromDb(mog.queryResults);
        } else {
            final String[] sentence = mog.queryResults.get(0).split(" ");
            return sentence[sentence.length - 1];
        }
    }

    /**
     * Get the parsed result count from the current query.
     * @return The parsed result count
     */
    private static int getParsedResultCount() {
        if (mog.queryResults.size() == 0 || (!mog.queryResults.get(0).contains(Constants.VALUES))) {
            return 0;
        } 
        final String[] sentence = mog.queryResults.get(0).split(" ");
        try {
            return Integer.parseInt(sentence[0]);
        } catch (Exception e) {
            return Integer.parseInt(sentence[1]);
        }
    }

    /**
     * Construct the test name string.
     * @param lineNumber The line number at which the query begins
     * @param queryString The query string
     * @param parsedHash The hash parsed from the trace file
     * @return The test name string
     */
    private static String buildTestNameString(final int lineNumber, final String queryString, final String parsedHash) {
        StringBuilder builder = new StringBuilder();
        builder.append("Line: ");
        builder.append(lineNumber);
        builder.append(" | Hash:");
        builder.append(parsedHash);
        return builder.toString();
    }

    /**
     * 
     * @param lineNumber
     * @param queryString
     * @param errorMessage
     * @return
     */
    private static String buildTestFailureString(final int lineNumber, final String queryString, final String errorMessage) {
        StringBuilder builder = new StringBuilder();
        builder.append("Failure at line ");
        builder.append(lineNumber);
        builder.append(": ");
        builder.append(errorMessage);
        builder.append('\n');
        builder.append(queryString);
        return builder.toString();        
    }
}