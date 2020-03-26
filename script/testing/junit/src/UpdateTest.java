/**
 * Update statement tests.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class UpdateTest extends TestUtility {
    private Connection conn;
    private ResultSet rs;

    /**
     * Setup for each test, make the default connection
     */
    @Before
    public void setup() throws SQLException {
        try {
            conn = makeDefaultConnection();
            conn.setAutoCommit(true);
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
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /*
     * ---------------------------------------------
     * Update statement tests
     * ---------------------------------------------
     */

   /**
     * Test that timestamps can be updated.
     * Fix #783: update query that modifies timestamp attribute fails.
     */
    @Test
    public void testUpdateTimestamp() throws SQLException {
        String ts1 = "2020-01-02 12:23:34.56789";
        String ts2 = "2020-01-02 11:22:33.721-05";
        String createSQL = "CREATE TABLE xxx (c1 int, c2 timestamp);";
        String insertSQL1 = "INSERT INTO xxx (c1, c2) VALUES (1, '" + ts1 + "');";
        String insertSQL2 = "INSERT INTO xxx (c1, c2) VALUES (2, '" + ts1 + "');";
        String updateSQL = "UPDATE xxx SET c2 = '" + ts2 + "' WHERE c1 = 2;";
        String selectSQL = "SELECT * from xxx ORDER BY c1 ASC;";
        String dropSQL = "DROP TABLE xxx";

        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.addBatch(createSQL);
        stmt.addBatch(insertSQL1);
        stmt.addBatch(insertSQL2);
        stmt.addBatch(updateSQL);
        stmt.executeBatch();
        conn.commit();
        conn.setAutoCommit(true);

        stmt = conn.createStatement();
        rs = stmt.executeQuery(selectSQL);

        rs.next();
        assertEquals(1, rs.getInt("c1"));
        assertEquals(ts1, rs.getTimestamp("c2").toString());
        rs.next();
        assertEquals(2, rs.getInt("c1"));
        assertEquals("2020-01-02 16:22:33.721", rs.getTimestamp("c2").toString());

        assertNoMoreRows(rs);

        stmt = conn.createStatement();
        stmt.execute(dropSQL);
    }

}
