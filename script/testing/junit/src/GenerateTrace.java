import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import moglib.*;


/**
 * class that convert sql statements to trace format
 * first, establish a local postgresql database
 * second, start the database server with "pg_ctl -D /usr/local/var/postgres start"
 * third, modify the url, user and password string to match the database you set up
 * finally, provide path to a file, run generateTrace with the file path as argument
 * input file format: sql statements, one per line
 * output file: to be tested by TracefileTest
 */
public class GenerateTrace {
    public static void main(String[] args) throws Throwable {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        String path = args[0];
        File file = new File(path);
        System.out.println("File path: " + path);
        MogSqlite mog = new MogSqlite(file);
        // open connection to postgresql database with jdbc
        MogDb db = new MogDb(args[1], args[2], args[3]);
        Connection conn = db.getDbTest().newConn();
        // remove existing table name
        List<String> tab = getAllExistingTableName(mog,conn);
        removeExistingTable(tab,conn);

        String line;
        String label;
        Statement statement = null;
        BufferedReader br = new BufferedReader(new FileReader(file));
        // create output file
        FileWriter writer = new FileWriter(new File(Constants.DEST_DIR, args[4]));
        int expected_result_num = -1;
        boolean include_result = false;
        while (null != (line = br.readLine())) {
            line = line.trim();
            // execute sql statement
            try{
                statement = conn.createStatement();
                statement.execute(line);
                label = Constants.STATEMENT_OK;
            } catch (Throwable e){
                label = Constants.STATEMENT_ERROR;
            }

            if(line.startsWith("SELECT")){
                // SELECT statement, query from database to construct trace format
                ResultSet rs = statement.getResultSet();
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
                String query_sort = Constants.QUERY + " " + typeString + " nosort";
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
    }

    public static void writeToFile(FileWriter writer, String str) throws IOException {
        writer.write(str);
        writer.write('\n');
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
