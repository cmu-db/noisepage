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
 * class that convert sql statements to trace format
 * first, establish a local postgesql database
 * second, start the database server with "pg_ctl -D /usr/local/var/postgres start"
 * third, provide path to a file, run generateTrace with the file path as argument
 * input file format: sql statements, one per line
 * output file: to be tested by TracefileTest
 */
public class generateTrace {
    private static final String STATEMENT_OK = "statement ok";
    public static final String QUERY_I_NOSORT = "query I nosort";
    public static final String SEPARATION = "----";

    public static void main(String[] args) throws Throwable {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        String path = args[0];
        File file = new File(path);
        MogSqlite mog = new MogSqlite(file);
        String url = "jdbc:postgresql://localhost/jeffdb";
        String user = "jeffniu";
        String password = "";
        MogDb db = new MogDb(url, user, password);
        // open connection to postgresql database specifying user and password with jdbc
        Connection conn = db.getDbTest().newConn();
        // remove existing table name
        List<String> tab = getAllExistingTableName(mog,db);
        removeExistingTable(tab,db);

        String line;
        BufferedReader br = new BufferedReader(new FileReader(file));
        // create output file
        FileWriter writer = new FileWriter(new File("script/testing/junit/src","select_output.test"));
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        while (null != (line = br.readLine())) {
            line = line.trim();
//            System.out.println(line);
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

    /**
     * query hash from the database
     * @param mog
     * @param db
     * @return
     * @throws SQLException
     * @throws IOException
     */
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
//        System.out.println("sdaasdasd   "+ rs.next());
//        System.out.println("sdaasdasd   "+ res);
        return res;
    }
}
