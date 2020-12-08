/**
 * Insert statement tests.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class WireTest extends TestUtility {
    private Connection conn;
    private ResultSet rs;
    private String S_SQL = "SELECT * FROM tbl;";

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE tbl (" +
                    "c1 double);";

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);
    }

    /**
     * Get columns, for follow on value checking
     */
    private void getResults() throws SQLException {
        Statement stmt = conn.createStatement();
        rs = stmt.executeQuery(S_SQL);
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
            } catch (SQLException e) {
                DumpSQLException(e);
            }
        }
    }

    /* --------------------------------------------
     * Insert statement tests
     * ---------------------------------------------
     */

    /**
     * 2 tuples inserted, do a bunch of reads. Extended Query with binary serialization enabled (see TestUtility.java)
     * will switch to binary serialization after 5 query invocations. This ensures we support text and binary format
     * for double precision floats
     */
    @Test
    public void testDouble() throws SQLException {
        PreparedStatement ps = conn.prepareStatement("INSERT INTO tbl VALUES (?), (?);");
        ps.setDouble(1, 15.445);
        ps.setDouble(2, 15.721);
        for (int i = 0; i < 100; i++) {
            ps.executeUpdate();
        }
        ps.close();

        ps = conn.prepareStatement("SELECT * FROM tbl");
        for (int i = 0; i < 100; i++) {
            rs = ps.executeQuery();
            for (int j = 0; j < 100; j++) {
                rs.next();
                assertEquals(15.445, rs.getDouble(1), 0.0001);
                rs.next();
                assertEquals(15.721, rs.getDouble(1), 0.0001);
            }
            assertNoMoreRows(rs);
            rs.close();
        }
        ps.close();
    }

}
