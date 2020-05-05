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

public class SequenceTest extends TestUtility {
    private Connection conn;
    private ResultSet rs;

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE tbl (dummy DECIMAL);";

    private static final String SQL_INSERT_TABLE =
            "INSERT INTO tbl (dummy) VALUES (1.0);";

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);
        stmt.execute(SQL_INSERT_TABLE);
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
     * Tests usage of sequence
     */
    @Test
    public void testSequence() throws SQLException {
        String create_SQL = "CREATE SEQUENCE seq;";
        Statement stmt = conn.createStatement();
        stmt.execute(create_SQL);
        String select_SQL = "SELECT nextval('seq') FROM tbl;";
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(select_SQL);
        boolean exists = rs.next();
        assert(exists);
        checkIntRow(rs, new String [] {"nextval(VARCHAR)"}, new int [] {1});
        assertNoMoreRows(rs);
        String drop_SQL = "DROP SEQUENCE seq;";
        stmt = conn.createStatement();
        stmt.execute(drop_SQL);
    }

}
