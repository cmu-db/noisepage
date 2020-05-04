/**
 * Insert statement tests.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.*;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("ALL")
public class CreateIndexTest extends TestUtility {
    private Connection conn1;
    private Connection conn2;

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
        Statement stmt = conn1.createStatement();
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
            conn1 = makeDefaultConnection();
            conn1.setAutoCommit(true);
            conn2 = makeDefaultConnection();
            conn2.setAutoCommit(true);
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
            Statement stmt = conn1.createStatement();
            stmt.execute(SQL_DROP_TABLE);
        } catch (SQLException e) {
            DumpSQLException(e);
        } finally {
            try {
                if (conn1 != null) {
                    conn1.close();
                }
                if (conn2 != null) {
                    conn2.close();
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
        Statement stmt = conn1.createStatement();
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
        Statement stmt = conn1.createStatement();
        int num_rows = 1000;
        for (int i = 0; i < num_rows; i++) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                throw e;
            }
        }
        Thread t2 = new Thread(() -> {
            try {
                Statement stmt2 = conn2.createStatement();
                for (int i = 0; i < num_rows; i++) {
                    stmt2.execute("INSERT INTO tbl VALUES (3, 4, 200);");
                }
            } catch(SQLException e) {
                DumpSQLException(e);
                Assert.fail();
            }
        });
        t2.start();
        stmt.execute("CREATE INDEX tbl_ind ON tbl (c2)");
        t2.join();
        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl WHERE c2 > 0 ORDER BY c2 ASC");
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 100});
        }
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {3, 4, 200});
        }
        for (int i = 0; i < num_rows; i++) {
            rs.next();
            checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {5, 6, 100});
        }
        assertNoMoreRows(rs);
    }

}
