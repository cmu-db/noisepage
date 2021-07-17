/**
 * GenerateTrace.java
 */

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;

import java.util.List;

import moglib.MogDb;
import moglib.MogSqlite;
import moglib.Constants;

/**
 * A generic logger interface.
 * (Apparently `Logger` is already taken)
 */
interface ILogger {
    public void info(final String message);
    public void error(final String message);
}

/**
 * A dummy logger class that just writes to standard output.
 * 
 * We might want to replace this eventually with an actual
 * logger implementation, and this dummy class might(?) make
 * that transition slightly less painful. For now, it also
 * provides the slight benefit of making logging less verbose.
 */
class StandardLogger implements ILogger {
    /**
     * Construct a logger instance.
     */
    StandardLogger() {}

    /**
     * Log an informational message.
     * @param message
     */
    public void info(final String message) {
        System.out.println(message);
    } 

    /**
     * Log an error message.
     * @param message
     */
    public void error(final String message) {
        System.err.println(message);
    }
}

/**
 * The GenerateTrace class converts SQL statements to the tracefile
 * format used for integration testing. For instructions on how to
 * use this program to generate a tracefile, see junit/README.
 */
public class GenerateTrace {
    /**
     * Error code for process exit on program success.
     */
    private static final int EXIT_SUCCESS = 0;

    /**
     * Error code for process exit on program failure.
     */
    private static final int EXIT_ERROR = 1;

    /**
     * The expected number of commandline arguments.
     */
    private static final int EXPECTED_ARGUMENT_COUNT = 5;

    /**
     * The character used to delimit multiline statements (e.g. UDF definition).
     */
    private static final String MULTILINE_DELIMITER = "\\";

    /**
     * The logger instance.
     */
    private static final ILogger LOGGER = new StandardLogger();

    /**
     * Program entry point.
     * @param args Commandline arguments
     * @throws Throwable
     */
    public static void main(String[] args) throws Throwable {
        if (args.length < EXPECTED_ARGUMENT_COUNT) {
            LOGGER.error("Error: invalid arguments");
            LOGGER.error("Usage: see junit/README.md");
            System.exit(EXIT_ERROR);
        }

        LOGGER.info("Working Directory = " + System.getProperty("user.dir"));
        
        final String path = args[0];
        File file = new File(path);
                
        MogSqlite mog = new MogSqlite(file);
        
        // Open connection to Postgre database over JDBC
        MogDb db = new MogDb(args[1], args[2], args[3]);
        Connection connection = db.getDbTest().newConn();
        
        // Initialize the database
        removeAllTables(mog, connection);
        removeAllFunctions(mog, connection);

        String line;
        String label;
        Statement statement = null;
        BufferedReader br = new BufferedReader(new FileReader(file));
        
        // Create output file
        FileWriter writer = new FileWriter(new File(Constants.DEST_DIR, args[4]));
        
        int expected_result_num = -1;
        boolean include_result = false;
        while (null != (line = readLine(br, MULTILINE_DELIMITER))) {
            line = line.trim();
            LOGGER.info(line);
            
            // Execute SQL statement
            try{
                statement = connection.createStatement();
                statement.execute(line);
                label = Constants.STATEMENT_OK;
            } catch (SQLException e) {
                System.err.println("Error executing SQL Statement: '" + line + "'; " + e.getMessage());
                label = Constants.STATEMENT_ERROR;
            } catch (Throwable e) {
                label = Constants.STATEMENT_ERROR;
            }

            if(line.startsWith("SELECT") || line.toLowerCase().startsWith("with")) {
                ResultSet rs = statement.getResultSet();
                if (line.toLowerCase().startsWith("with") && null == rs) {
                    // We might have a query that begins with `WITH` that has a null result set
                    int updateCount = statement.getUpdateCount();
                    // check if expected number is equal to update count
                    if(expected_result_num>=0 && expected_result_num!=updateCount){
                        label = Constants.STATEMENT_ERROR;
                    }
                    writeToFile(writer, label);
                    writeToFile(writer, line);
                    writer.write('\n');
                    expected_result_num = -1;
                    continue;
                }

                ResultSetMetaData rsmd = rs.getMetaData();
                String typeString = "";
                for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
                    String colTypeName = rsmd.getColumnTypeName(i);
                    MogDb.DbColumnType colType = db.getDbTest().getDbColumnType(colTypeName);
                    if(colType==MogDb.DbColumnType.FLOAT){
                        typeString += "R";
                    }else if(colType==MogDb.DbColumnType.INTEGER){
                        typeString += "I";
                    }else if(colType==MogDb.DbColumnType.TEXT){
                        typeString += "T";
                    }else{
                        System.out.println(colTypeName + " column invalid");
                    }
                }

                String sortOption;
                if (line.contains("ORDER BY")) {
                    // These rows are already sorted by the SQL and need to match exactly
                    sortOption = "nosort";
                    mog.sortMode = "nosort";
                } else {
                    // Need to create a canonical ordering...
                    sortOption = "rowsort";
                    mog.sortMode = "rowsort";
                }
                String query_sort = Constants.QUERY + " " + typeString + " " + sortOption;
                writeToFile(writer, query_sort);
                writeToFile(writer, line);
                writeToFile(writer, Constants.SEPARATION);
                List<String> res = mog.processResults(rs);
                // compute the hash
                String hash = TestUtility.getHashFromDb(res);
                String queryResult = "";
                // when include_result is true, set queryResult to be exact result instead of hash
                if(include_result){
                    for(String i:res){
                        queryResult += i;
                        queryResult += "\n";
                    }
                    queryResult = queryResult.trim();
                }else{
                    // if expected number of results is specified
                    if(expected_result_num>=0){
                        queryResult = "Expected " + expected_result_num + " values hashing to " + hash;
                    }else{
                        if(res.size()>0){
                            // set queryResult to format x values hashing to xxx
                            queryResult = res.size() + " values hashing to " + hash;
                        }
                        // set queryResult to be exact result instead of hash when
                        // result size is smaller than Constants.DISPLAY_RESULT_SIZE
                        if(res.size() < Constants.DISPLAY_RESULT_SIZE){
                            queryResult = "";
                            for(String i:res){
                                queryResult += i;
                                queryResult += "\n";
                            }
                            queryResult = queryResult.trim();
                        }
                    }
                }
                writeToFile(writer, queryResult);
                if(res.size()>0){
                    writer.write('\n');
                }
                include_result = false;
                expected_result_num = -1;
            } else if(line.startsWith(Constants.HASHTAG)){
                writeToFile(writer, line);
                if(line.contains(Constants.NUM_OUTPUT_FLAG)){
                    // case for specifying the expected number of outputs
                    String[] arr = line.split(" ");
                    expected_result_num = Integer.parseInt(arr[arr.length-1]);
                }else if(line.contains(Constants.FAIL_FLAG)){
                    // case for expecting the query to fail
                    label = Constants.STATEMENT_ERROR;
                } else if(line.contains(Constants.EXPECTED_OUTPUT_FLAG)){
                    // case for including exact result in mog.queryResult
                    include_result = true;
                }
            } else{
                // other sql statements
                int rs = statement.getUpdateCount();
                // check if expected number is equal to update count
                if(expected_result_num>=0 && expected_result_num!=rs){
                    label = Constants.STATEMENT_ERROR;
                }
                writeToFile(writer, label);
                writeToFile(writer, line);
                writer.write('\n');
                expected_result_num = -1;
            }
        }
        writer.close();
        br.close();

        System.exit(EXIT_SUCCESS);
    }

    /**
     * Read a line from the specified `BufferedReader` instance.
     * @param reader The instance from which lines are read
     * @param delimiter The character used to delimit multiline statements
     * @return The input line, or `null` on end of input
     */
    private static String readLine(BufferedReader reader, final String delimiter) throws IOException {    
        StringBuilder builder = new StringBuilder();
        for (;;) {
            final String input = reader.readLine();
            if (input == null) {
                return null;
            }

            if (input.endsWith(delimiter)) {
                builder.append(
                    input.substring(0, input.length() - delimiter.length() - 1)
                         .trim() + " ");
            } else {
                builder.append(input);
                break;
            }
        }
        return builder.toString();
    }

    /**
     * Write the specified line to a file using the provided `FileWriter`.
     * @param writer The `FileWriter` instance
     * @param line The line to be written
     * @throws IOException On IO error
     */
    public static void writeToFile(FileWriter writer, final String line) throws IOException {
        writer.write(line);
        writer.write('\n');
    }

    /* ------------------------------------------------------------------------
        Table Management
    ------------------------------------------------------------------------ */

    /**
     * Remove all existing tables from the database
     * @param mog The `MogSqlite` instance
     * @param connection The database connection
     * @throws SQLException On SQL error
     */
    private static void removeAllTables(MogSqlite mog, Connection connection) throws SQLException {
        final List<String> tableNames = getExistingTableNames(mog, connection);
        removeTables(tableNames, connection);
    }   

    /**
     * Get the names of all existing tables in the database.
     * @param mog The `MogSqlite` instance
     * @param connection The database connection
     * @return A list of all table names
     * @throws SQLException On SQL exception
     */
    public static List<String> getExistingTableNames(MogSqlite mog, Connection connection) throws SQLException {
        final String query = "SELECT TABLENAME FROM pg_tables WHERE schemaname = 'public';";
        Statement statement = connection.createStatement();
        statement.execute(query);
        return mog.processResults(statement.getResultSet());
    }

    /**
     * Remove all specified tables from the database.
     * @param tableNames The collection of table names to remove
     * @param connection The database connection
     * @throws SQLException On SQL error
     */
    private static void removeTables(final List<String> tableNames, Connection connection) throws SQLException {
        for (final String tableName : tableNames){
            removeTable(tableName, connection);
        }
    }

    /**
     * Remove the specified table from the database.
     * @param tableName The name of the table to remove
     * @param connection The database connection
     * @throws SQLException On SQL error
     */
    private static void removeTable(final String tableName, Connection connection) throws SQLException {
        final String query = "DROP TABLE IF EXISTS " + tableName + " CASCADE";
        Statement statement = connection.createStatement();
        statement.execute(query);
    }

    /* ------------------------------------------------------------------------
        Function Management
    ------------------------------------------------------------------------ */

    /**
     * Remove all existing functions from the database.
     * @param mog The `MogSqlite` instance.
     * @param connection The database connection.
     * @throws SQLException On SQL error
     */
    private static void removeAllFunctions(MogSqlite mog, Connection connection) throws SQLException {
        final List<String> functionNames = getExistingFunctions(mog, connection);
        removeFunctions(functionNames, connection);
    }

    /**
     * Get the names of all existing functions in the database.
     * @param mog The MogSqlite instance
     * @param connection The databse connection
     * @return A collection of the function names
     * @throws SQLException On SQL error
     */
    private static List<String> getExistingFunctions(MogSqlite mog, Connection connection) throws SQLException {
        final String query = "SELECT proname FROM pg_proc WHERE pronamespace = 'public'::regnamespace;";
        Statement statement = connection.createStatement();
        statement.execute(query);
        return mog.processResults(statement.getResultSet());
    }

    /**
     * Remove all of the functions in `functionNames` from the database.
     * @param functionNames The names of the functions to remove
     * @param connection The database connection
     * @throws SQLException On SQL error
     */
    private static void removeFunctions(final List<String> functionNames, Connection connection) throws SQLException {
        for (final String functionName : functionNames) {
            removeFunction(functionName, connection);
        }
    }   

    /**
     * Remove the function identified by `functionName` from the database.
     * @param functionName The name of the function to remove
     * @param connection The database connection
     * @throws SQLException On SQL error
     */
    private static void removeFunction(final String functionName, Connection connection) throws SQLException {
        final String query = "DROP FUNCTION IF EXISTS " + functionName + " CASCADE;";
        Statement statement = connection.createStatement();
        statement.execute(query);
    }
}
