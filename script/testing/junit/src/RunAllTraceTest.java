import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class RunAllTraceTest {
    @Test
    public void runTraces() throws IOException, SQLException {
        String curDirectory = System.getProperty("user.dir");
        String traceDirectory = curDirectory + "/noisepage-testfiles/sql_trace";
        File[] files = new File(traceDirectory).listFiles();
        for(File file : files){
            if(file.isDirectory()){
                String typeDirectory = traceDirectory + "/" + file.getName();
                File[] typeFiles = new File(typeDirectory).listFiles();
                for(File tf : typeFiles){
                    if(tf.getName().contains("output")){
                        String fileDirectory = typeDirectory + "/" + tf.getName();
                        TracefileT cur = new TracefileT(fileDirectory);
                        System.out.println(fileDirectory);
                        cur.execute();
                    }
                }
            }
        }
    }
//    public static void main(String[] args) throws IOException, SQLException {
//        runTraces();
//
//    }
}
