import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import moglib.*;
import org.junit.Test;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * class that filter out desired trace
 * check out instruction at junit/README.md
 */
public class FilterTrace {
    public static final String QUERY = "query";
    public static final String SEPARATION = "----";
    public static final String SKIP = "skip";
    public static final String DEST_DIR = "traces";

    public static void main(String[] args) throws Throwable {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        String path = args[0];
        File file = new File(path);
        System.out.println("File path: " + path);
        String[] outputArr = path.split("/");
        String outputName = outputArr[outputArr.length-1] + "_new";
        MogSqlite mog = new MogSqlite(file);
        // create output file
        File outputFile = new File(DEST_DIR, outputName);
        FileWriter writer = new FileWriter(outputFile);
        String[] skip_list = args[4].split(",");
        for(String i: skip_list){
            System.out.println(i);
        }
        boolean skip_flag = false;
        // open connection to postgresql database with jdbc
        MogDb db = new MogDb(args[1], args[2], args[3]);
        Connection conn = db.getDbTest().newConn();
        List<String> tab = getAllExistingTableName(mog,conn);
        removeExistingTable(tab,conn);
        while (mog.next()) {
            String cur_sql = mog.sql.trim();
            for(String i:mog.comments){
                writeToFile(writer, i);
            }
            for(String skip_word:skip_list){
                if(cur_sql.contains(skip_word)){
                    skip_flag = true;
                    writeToFile(writer, SKIP);
                    break;
                }
            }
            writeToFile(writer, mog.queryFirstLine);
            writeToFile(writer, cur_sql);
            if(skip_flag){
                if(mog.queryFirstLine.contains(QUERY)){
                    writeToFile(writer, SEPARATION);
                    if(mog.queryResults.size()>0){
                        writeToFile(writer, mog.queryResults.get(0));
                    }
                }
            }else{
                if(mog.queryFirstLine.contains(QUERY)){
                    writeToFile(writer, SEPARATION);
                    try{
                        Statement statement = conn.createStatement();
                        statement.execute(cur_sql);
                        ResultSet rs = statement.getResultSet();
                        List<String> res = mog.processResults(rs);
                        if(res.size()==0){
                            writer.write('\n');
                        }else{
                            String hash = TestUtility.getHashFromDb(res);
                            String queryResult = res.size() + " values hashing to " + hash;
                            writeToFile(writer, queryResult);
                        }
                    }catch(Throwable e){
                        throw new Throwable(e.getMessage() + ": " + cur_sql);
                    }
                }else{
                    try{
                        Statement statement = conn.createStatement();
                        statement.execute(cur_sql);
                    }catch(Throwable e){
                        throw new Throwable(e.getMessage() + ": " + cur_sql);
                    }
                }
            }
            writer.write('\n');
            skip_flag = false;
            mog.comments.clear();
            mog.queryResults.clear();
        }
        writer.close();
        conn.close();
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
