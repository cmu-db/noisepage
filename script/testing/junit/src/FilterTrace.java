import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import moglib.*;

/**
 * class that filter out desired trace
 * check out instruction at junit/README.md
 */
public class FilterTrace {
    public static void main(String[] args) throws Throwable {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        String path = args[0];
        File file = new File(path);
        System.out.println("File path: " + path);
        MogSqlite mog = new MogSqlite(file);
        // create output file
        File outputFile = new File(Constants.DEST_DIR, args[5]);
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
            for(int i=0; i<mog.comments.size();i++){
                if(mog.comments.get(i).contains(Constants.SKIPIF)||mog.comments.get(i).contains(Constants.ONLYIF)){
                    skip_flag = true;
                    break;
                }
            }
            // filter out nested SELECT statements
            if(getFrequency(cur_sql, "SELECT")>1){
                mog.comments.clear();
                continue;
            }
            // the code below remove the queries that contain any skip keyword
            for(String skip_word:skip_list) {
                if (cur_sql.contains(skip_word)) {
                    skip_flag = true;
                    break;
                }
            }
            if(skip_flag){
                skip_flag = false;
                mog.comments.clear();
                continue;
            }
            writeToFile(writer, mog.queryFirstLine);
            writeToFile(writer, cur_sql);
            if(mog.queryFirstLine.contains(Constants.QUERY)){
                writeToFile(writer, Constants.SEPARATION);
                try{
                    Statement statement = conn.createStatement();
                    statement.execute(cur_sql);
                    ResultSet rs = statement.getResultSet();
                    List<String> res = mog.processResults(rs);
                    if(res.size()>0){
                        if(res.size()<Constants.DISPLAY_RESULT_SIZE) {
                            for(String i:mog.queryResults){
                                writeToFile(writer, i);
                            }
                        }else {
                            String hash = TestUtility.getHashFromDb(res);
                            String queryResult = res.size() + " values hashing to " + hash;
                            writeToFile(writer, queryResult);
                        }
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
            writer.write('\n');
            mog.comments.clear();
            mog.queryResults.clear();
        }
        writer.close();
        conn.close();
    }
    public static int getFrequency(String sql, String keyword){
        int num = 0;
        String[] arr = sql.split(" ");
        for(String i:arr){
            if(i.contains(keyword)){
                num += 1;
            }
        }
        return num;
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
