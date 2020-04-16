/**
 * Insert statement tests.
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

public class BuiltinUDFTest extends TestUtility {
    private Connection conn;
    private ResultSet rs;
    private String S_SQL = "SELECT * FROM tbl;";

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE tbl (" +
                    "theta DECIMAL(5,2));";

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
        conn = makeDefaultConnection();
        conn.setAutoCommit(true);
        initDatabase();
    }

    /**
     * Cleanup for each test, execute after each test
     * drop the default table
     */
    @After
    public void teardown() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
    }

    /* --------------------------------------------
     * UDF statement tests
     * ---------------------------------------------
     */

    /**
     * Tests usage of trig udf functions
     * #744 test
     */
    @Test
    public void testBuiltinUDFTrig() throws SQLException {
        String insert_SQL = "INSERT INTO tbl (theta) VALUES (0.0);";
        Statement stmt = conn.createStatement();
        stmt.execute(insert_SQL);
        String select_SQL = "SELECT cos(theta) FROM tbl;";
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(select_SQL);
        boolean exists = rs.next();
        assert(exists);
        checkDoubleRow(rs, new String [] {"cos(theta)"}, new double [] {1.0});
        assertNoMoreRows(rs);

        select_SQL = "SELECT sin(theta) FROM tbl;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        exists = rs.next();
        assert(exists);
        checkDoubleRow(rs, new String [] {"sin(theta)"}, new double [] {0.0});
        assertNoMoreRows(rs);
    }

}
