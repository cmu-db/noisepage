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
    private static final String STATEMENT_OK = "statement ok";
    public static final String QUERY_I_NOSORT = "query I nosort";
    public static final String SEPARATION = "----";

    public static void main(String[] args) throws Throwable {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        String path = args[0];
        File file = new File(path);
        MogSqlite mog = new MogSqlite(file);
        System.out.println(args[1]);
        System.out.println(args[2]);
        // open connection to postgresql database with jdbc
        String url = args[1];
        String user = args[2];
        String password = "";
        MogDb db = new MogDb(url, user, password);
        Connection conn = db.getDbTest().newConn();
        // remove existing table name
        List<String> tab = getAllExistingTableName(mog,conn);
        removeExistingTable(tab,conn);

        String line;
        BufferedReader br = new BufferedReader(new FileReader(file));
        // create output file
        FileWriter writer = new FileWriter(new File("script/testing/junit/src","hahaha_output.test"));
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        while (null != (line = br.readLine())) {
            line = line.trim();
            // execute sql statement
            Statement statement = conn.createStatement();
            statement.execute(line);
            if(line.startsWith("SELECT")){
                // SELECT statement, query from database to construct trace format
                String[] lines = line.split(",");
                writeToFile(writer, QUERY_I_NOSORT);
                writeToFile(writer, line);
                writeToFile(writer, SEPARATION);
                ResultSet rs = statement.getResultSet();
                List<String> res = mog.processResults(rs);
                // compute the hash
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
                // other sql statements
                writeToFile(writer, STATEMENT_OK);
                writeToFile(writer, line);
                writer.write('\n');
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
