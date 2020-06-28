import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class RunAllTraceTester {
//    @Test
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
//                        TracefileTester cur = new TracefileTester(fileDirectory);
                        TracefileTest temp = new TracefileTest();

                        System.out.println(fileDirectory);
//                        cur.execute();
                        // TODO:
                    }
                }
            }
        }
    }
}
