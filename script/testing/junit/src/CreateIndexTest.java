/**
 * Insert statement tests.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.*;

import javax.xml.transform.Result;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("ALL")
public class CreateIndexTest extends TestUtility {
    private static final int NUM_EXTRA_THREADS = 2;

    private Connection conn;
    private Connection[] thread_conn = new Connection[NUM_EXTRA_THREADS];

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE tbl (" +
                    "c1 int, " +
                    "c2 int," +
                    "c3 int);";

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);
    }

    /**
     * Setup for each test, execute before each test
     * reconnect and setup default table
     */
    @Before
    public void setup() throws SQLException {
        try {
            conn = makeDefaultConnection();
            conn.setAutoCommit(true);
            for (int i = 0; i < NUM_EXTRA_THREADS; i++) {
                thread_conn[i] = makeDefaultConnection();
                thread_conn[i].setAutoCommit(true);
            }
            initDatabase();
        } catch (SQLException e) {
            DumpSQLException(e);
        }
    }

    /**
     * Cleanup for each test, execute after each test
     * drop the default table and close connection
     */
    @After
    public void teardown() throws SQLException {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(SQL_DROP_TABLE);
        } catch (SQLException e) {
            DumpSQLException(e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                for (int i = 0; i < NUM_EXTRA_THREADS; i++) {
                    thread_conn[i].close();
                }
            } catch (SQLException e) {
                DumpSQLException(e);
            }
        }
    }

    /* --------------------------------------------
     * Create index tests
     * ---------------------------------------------
     */

    /**
     * Does a simple create index on a populated table
     */
    @Test
    public void testSimpleCreate() throws SQLException {
        String sql = "INSERT INTO tbl VALUES (1, 2, 100), (5, 6, 100), (3, 4, 100);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.execute("CREATE INDEX tbl_ind ON tbl (c2)");
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c2 > 0 ORDER BY c2 ASC");
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 100});
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {3, 4, 100});
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {5, 6, 100});
        assertNoMoreRows(rs);
    }

    /**
     * Does a simple create index on a populated table
     */
    @Test
    public void testWriteBlocking() throws SQLException, InterruptedException {
        String sql = "INSERT INTO tbl VALUES (1, 2, 100), (5, 6, 100);";
        Statement stmt = conn.createStatement();
        int num_rows = 1500;
        for (int i = 0; i < num_rows; i++) {
            stmt.execute(sql);
        }
        Thread[] threads = new Thread[NUM_EXTRA_THREADS];
        for (int i = 0; i < NUM_EXTRA_THREADS; i++) {
            final Connection conn2 = thread_conn[i];
            final int i2  = i;
            thread_conn[i].setAutoCommit(i % 2 == 0);
            threads[i] = new Thread(() -> {
                try {
                    Statement stmt2 = conn2.createStatement();
                    for (int j = 0; j < num_rows; j++) {
                        stmt2.execute("INSERT INTO tbl VALUES (3, 4, 200);");
                    }
                    if(i2 % 2 == 1) {
                        conn2.commit();
                    }
                } catch(SQLException e) {
                    DumpSQLException(e);
                    Assert.fail();
                }
            });
            threads[i].start();
        }
        Thread.sleep(100);
        stmt.execute("CREATE INDEX tbl_ind ON tbl (c2)");
        for (Thread t : threads) {
            t.join();
        }
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c2 > 0 ORDER BY c2 ASC");
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 100});
        }
        for (int i = 0; i < num_rows * NUM_EXTRA_THREADS; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {3, 4, 200});
        }
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {5, 6, 100});
        }
        assertNoMoreRows(rs);
    }

    /**
     * Checks to see if delete propagates to index
     */
    @Test
    public void testSimpleDelete() throws SQLException {
        String sql = "INSERT INTO tbl VALUES (1, 2, 100), (5, 6, 102);";
        Statement stmt = conn.createStatement();
        int num_rows = 500;
        for (int i = 0; i < num_rows; i++) { stmt.execute(sql); }

        // This will be the tuple that we later delete
        stmt.execute("INSERT INTO tbl VALUES (3, 4, 101);");

        stmt.execute("CREATE INDEX tbl_ind on tbl (c2);");
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c2 > 0 ORDER BY c2 ASC;");
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 100});
        }
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {3, 4, 101});
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {5, 6, 102});
        }
        assertNoMoreRows(rs);

        stmt.execute("DELETE FROM tbl WHERE c3 = 101;");
        rs = stmt.executeQuery("SELECT * FROM tbl WHERE c2 > 0 ORDER BY c2 ASC;");
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 100});
        }

        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {5, 6, 102});
        }
        assertNoMoreRows(rs);
    }


    /**
     * Checks to see if delete propagates to index
     */
    @Test
    public void testConcurrentDelete() throws SQLException, InterruptedException {
        String sql = "INSERT INTO tbl VALUES (";
        Statement stmt = conn.createStatement();
        int num_rows = 500;
        for (int i = 0; i < NUM_EXTRA_THREADS * num_rows; i++) {
            stmt.execute(sql + (i + 5) + ", 2, 100);");
        }

        Thread[] threads = new Thread[NUM_EXTRA_THREADS];
        for (int i = 0; i < NUM_EXTRA_THREADS; i++) {
            final Connection conn2 = thread_conn[i];
            final int i2 = i;
            threads[i] = new Thread(() -> {
                try {
                    Statement stmt2 = conn2.createStatement();
                    if(i2 % 2 == 0) {
                        for (int j = 0; j < num_rows; j++) {
                            stmt2.execute("INSERT INTO tbl VALUES (3, 4, 200);");
                        }
                    } else {
                        for(int j = 0; j < num_rows; j++) {
                            stmt2.execute("DELETE FROM tbl WHERE c1 =" + (num_rows * i2 + j + 5));
                        }
                    }
                } catch(SQLException e) {
                    DumpSQLException(e);
                    Assert.fail();
                }
            });
            threads[i].start();
        }
        Thread.sleep(100);
        stmt.execute("CREATE INDEX tbl_ind ON tbl (c1)");
        for (Thread t : threads) {
            t.join();
        }
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c1 > 0 ORDER BY c1 ASC");
        for (int i = 0; i < num_rows * ((NUM_EXTRA_THREADS + 1) / 2); i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {3, 4, 200});
        }
        for (int i = 0; i < NUM_EXTRA_THREADS * num_rows; i++) {
            if (i / num_rows % 2 != 0) continue;
            rs.next();
            //System.out.println(i + ": " + rs.getInt(1));
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {i + 5, 2, 100});
}
        assertNoMoreRows(rs);
    }

     /**
      * Checks to see if an aborting transaction doesn't update the index
      */
    @Test
    public void testSimpleAbort() throws SQLException, InterruptedException {
        String sql = "INSERT INTO tbl VALUES (1, 2, 100), (5, 6, 100);";
        Statement stmt = conn.createStatement();
        int num_rows = 500;
        for (int i = 0; i < num_rows; i++) {
            stmt.execute(sql);
        }

        Thread[] threads = new Thread[NUM_EXTRA_THREADS];
        for (int i = 0; i < NUM_EXTRA_THREADS; i++) {
            thread_conn[i].setAutoCommit(false);
            final Connection conn2 = thread_conn[i];
            final int i2 = i;
            threads[i] = new Thread(() -> {
                try {
                    Statement stmt2 = conn2.createStatement();
                    for (int j = 0; j < num_rows; j++) {
                        if (i2 == 0) { stmt2.execute("INSERT INTO tbl VALUES (3, 4, 200);"); }
                        else { stmt2.execute("INSERT INTO tbl VALUES (7, 8, 200);"); }
                    }

                    if(i2 % 2 == 0) {
                        Thread.sleep(30);
                        stmt2.execute("ROLLBACK");
                    } else {
                        stmt2.execute("COMMIT");
                    }
                } catch(SQLException e) {
                    DumpSQLException(e);
                    Assert.fail();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            });
            threads[i].start();
        }
        Thread.sleep(100);
        stmt.execute("CREATE INDEX tbl_ind ON tbl (c2)");
        for (Thread t : threads) {
            t.join();
        }
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c2 > 0 ORDER BY c2 ASC");
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 100});
        }
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {5, 6, 100});
        }
        for (int i = 0; i < num_rows * (NUM_EXTRA_THREADS/2); i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {7, 8, 200});
        }
        assertNoMoreRows(rs);
    }


    /**
     * Ensures proper MVCC semantics are still met even with the index being created
     */
    @Test
    public void testMVCC() throws SQLException, InterruptedException {
        String sql = "INSERT INTO tbl VALUES (1, 2, 100), (5, 6, 102);";
        Statement stmt = conn.createStatement();
        int num_rows = 1000;
        for (int i = 0; i < num_rows; i++) { stmt.execute(sql); }

        conn.setAutoCommit(false);
        Statement select = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl where c2 > 0 ORDER BY c2 ASC");
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 100});
        }

        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {5, 6, 102});
        }
        assertNoMoreRows(rs);

        Thread[] threads = new Thread[NUM_EXTRA_THREADS];
        for (int i = 0; i < NUM_EXTRA_THREADS; i++) {
            final Connection conn2 = thread_conn[i];
            final int i2  = i;
            threads[i] = new Thread(() -> {
                try {
                    Statement stmt2 = conn2.createStatement();
                    for (int j = 0; j < num_rows; j++) {
                        stmt2.execute("INSERT INTO tbl VALUES (3, 4, 200);");
                    }
                } catch(SQLException e) {
                    DumpSQLException(e);
                    Assert.fail();
                }
            });
            threads[i].start();
        }

        Connection indexConn = makeDefaultConnection();
        indexConn.setAutoCommit(true);
        indexConn.createStatement().execute("CREATE INDEX tbl_ind on tbl (c2)");
        indexConn.close();

        rs = stmt.executeQuery("SELECT * FROM tbl WHERE c2 > 0 ORDER BY c2 ASC;");

        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 100});
        }

        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {5, 6, 102});
        }
        assertNoMoreRows(rs);
        conn.commit();
        for (Thread t : threads) {
            t.join();
        }
    }
}
