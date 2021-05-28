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
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    // Constants
    // ------------------------------------------------------------------------

    /**
     * The required precision for floating point results.
     */
    private static final double PRECISION = 0.00001;

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

        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append("Line: ");
        nameBuilder.append(lineNumber);
        nameBuilder.append(" | Hash:");
        nameBuilder.append(expectedHash);
        final String testName = nameBuilder.toString();

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
                final List<String> expectedResults = new ArrayList<>(mog.queryResults);
                exec = () -> checkResultSets(results, expectedResults);
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
                final boolean resultCountsMatch = checkResultCount(checkExpectedLength, expectedResultCount, results.size());
                exec = () -> checkResultHashes(expectedHash, resultHash, message, resultCountsMatch, results);
            }
        } catch (Throwable e) {
            StringBuilder builder = new StringBuilder();
            builder.append("Failure at line ");
            builder.append(lineNumber);
            builder.append(": ");
            builder.append(e.getMessage());
            builder.append('\n');
            builder.append(queryString);
            exec = () -> checkAlwaysFail(builder.toString());
        }

        return DynamicTest.dynamicTest(testName, exec);
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
     * Determine if the results of the query match the expected results.
     * @param results The results from the query executed against NoisePage
     * @param expectedResults The expected results from the tracefile
     * @throws RuntimeException
     */
    private static void checkResultSets(final List<String> results, final List<String> expectedResults) throws RuntimeException {
        if (results.size() != expectedResults.size()) {
            throw new RuntimeException("Unexpected number of results for query");
        }

        for (int i = 0; i < results.size(); ++i) {
            if (results.get(i) == null) {
                if (expectedResults.get(i) != null) {
                    throw new RuntimeException("Value null Mismatch");
                }
            } else if (results.get(i).isEmpty()) {
                if (!expectedResults.get(i).isEmpty()) {
                    throw new RuntimeException("Value '' Mismatch");
                }
            } else {
                try {
                    final double one = Double.parseDouble(results.get(i));
                    final double two = Double.parseDouble(expectedResults.get(i));
                    if (Math.abs(one - two) > PRECISION) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("Expected ");
                        builder.append(expectedResults);
                        builder.append(" but have ");
                        builder.append(results);
                        throw new RuntimeException(builder.toString());
                    }
                } catch (Exception e) {
                    if (!results.get(i).equals(expectedResults.get(i))) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("Expected ");
                        builder.append(expectedResults);
                        builder.append(" but have ");
                        builder.append(results);
                        throw new RuntimeException(builder.toString());
                    }
                }
            }
        }

        // Reaching this point indicates that all checks passed!
    }

    /**
     * Determine if the length of queried result equals length of expected result.
     * @param checkExpectedLength A boolean flag indicating whether the check is actually performed
     * @param expectedCount parsed result length
     * @param actualCount actual length
     * @return `true` if result counts match or the check is elided, `false` otherwise
     */
    private static boolean checkResultCount(final boolean checkExpectedLength, final int expectedCount, final int actualCount) {
        // If checkExpectedLength is `false` we elide the check entirely
        // TODO(Kyle): This logic is insane, why do we do this at all?
        return checkExpectedLength ? (expectedCount == actualCount) : true;
    }

    /**
     * Determine if the hashes for the actual and expected query result sets match.
     * @param expectedHash The expected hash from the tracefile
     * @param actualHash The hash computed at test time
     * @param message The error message
     * @param resultCountsMatch Indicates whether the result count check passed
     * @param results The queried result set
     * @throws RuntimeException
     */
    private static void checkResultHashes(final String expectedHash, final String actualHash, final String message,
                             final boolean resultCountsMatch, final List<String> results) throws RuntimeException {
        // If length doesn't match, throw
        if (!resultCountsMatch){
            throw new RuntimeException("Query got wrong number of values");
        }

        // try comparing the hash
        if (!actualHash.equals(expectedHash)) {
            // if hash doesn't match
            if (resultCountsMatch) {
                // If result counts match, we cast the double to int to
                // compare again (hack for float precision errors)
                List<String> updatedResults = new ArrayList<>();
                for (final String i : results) {
                    try {
                        final Integer value = Integer.valueOf((int) Math.round(Double.parseDouble(i)));
                        updatedResults.add(value.toString());
                    } catch (Exception ex) {
                        updatedResults.add(i);
                    }
                }
                final String updatedHash = TestUtility.getHashFromDb(updatedResults);
                if (!updatedHash.equals(expectedHash)) {
                    StringBuilder builder = new StringBuilder();
                    builder.append(message);
                    builder.append("\nExpected: ");
                    builder.append(expectedHash);
                    builder.append("\nActual: ");
                    builder.append(updatedHash);
                    builder.append('\n');
                    builder.append(results);
                    throw new RuntimeException(builder.toString());
                }
            } else {
                // Hashes and result counts disagree; throw
                StringBuilder builder = new StringBuilder();
                builder.append(message);
                builder.append("\nExpected: ");
                builder.append(expectedHash);
                builder.append("\nActual: ");
                builder.append(actualHash);
                throw new RuntimeException(builder.toString());
            }
        }

        // If we reach this point, the checks have passed!
    }

    /**
     * Wrapper for throwing message for the case in which SQL statements fail.
     * @param message The error message
     * @throws RuntimeException
     */
    private static void checkAlwaysFail(final String message) throws RuntimeException {
        throw new RuntimeException(message);
    }

    // ------------------------------------------------------------------------
    // Misc. Utilities
    // ------------------------------------------------------------------------

    /**
     * Get all sql query statement start line numbers.
     * @param input The input tracefile
     * @return A list of integers that contains start line numbers
     * @throws IOException
     */
    private static List<Integer> getQueryLineNumbers(File input) throws IOException {
        try (BufferedReader bf = new BufferedReader(new FileReader(input))) {
            List<Integer> lineNumbers = new ArrayList<>();
            String line;
            int counter = 0;
            while (null != (line = bf.readLine())){
                counter++;
                if (line.startsWith(Constants.QUERY)
                || line.startsWith(Constants.STATEMENT_ERROR)
                || line.startsWith(Constants.STATEMENT_OK)) {
                    lineNumbers.add(counter);
                }
            }
            return lineNumbers;
        }
    }

    /**
     * Determine if we should only check the expected length of the result sets.
     * @return `true` if the length of the result set should be checked, `false` otherwise
     */
    private static boolean getCheckExpectedLength() {
        // TODO(Kyle): I just ripped this logic out of the above
        // function, but this could still really use a deeper refactor
        if (mog.queryResults.size() == 0 || (!mog.queryResults.get(0).contains(Constants.VALUES))) {
            return false;
        }

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
        if (mog.queryResults.size() == 0 || (!mog.queryResults.get(0).contains(Constants.VALUES))) {
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
}
