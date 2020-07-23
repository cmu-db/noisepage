import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    private static final String STATEMENT_OK = "statement ok";
    private static final String STATEMENT_ERROR = "statement error";
    public static final String QUERY_I_NOSORT = "query I nosort";
    public static final String SEPARATION = "----";
    public static final String DEST_DIR = "traces";
    public static final String DEST_NAME = "output.test";

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
        FileWriter writer = new FileWriter(new File(DEST_DIR, DEST_NAME));
        while (null != (line = br.readLine())) {
            line = line.trim();
            // execute sql statement
            try{
                statement = conn.createStatement();
                statement.execute(line);
                label = STATEMENT_OK;
            } catch (Throwable e){
                label = STATEMENT_ERROR;
            }

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
            } else if(line.startsWith("#")){
                writeToFile(writer, line);
            } else{
                // other sql statements
                writeToFile(writer, label);
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
