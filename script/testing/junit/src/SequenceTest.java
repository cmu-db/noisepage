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
     * Tests usage of a default sequence
     */
    @Test
    public void testSequenceDefault() throws SQLException {
        String create_SQL = "CREATE SEQUENCE seq;";
        Statement stmt = conn.createStatement();
        stmt.execute(create_SQL);
        String nextval_SQL = "SELECT nextval('seq') FROM tbl;";
        String currval_SQL = "SELECT currval('seq') FROM tbl;";
        for (int i = 1; i < 10; i++) {
            // nextval
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(nextval_SQL);
            boolean exists = rs.next();
            assert (exists);
            checkIntRow(rs, new String[]{"nextval(VARCHAR)"}, new int[]{i});
            assertNoMoreRows(rs);
            // currval
            stmt = conn.createStatement();
            rs = stmt.executeQuery(currval_SQL);
            exists = rs.next();
            assert (exists);
            checkIntRow(rs, new String[]{"currval(VARCHAR)"}, new int[]{i});
            assertNoMoreRows(rs);
        }
        String drop_SQL = "DROP SEQUENCE seq;";
        stmt = conn.createStatement();
        stmt.execute(drop_SQL);
    }

    /**
     * Tests usage of a sequence with parameters
     */
    @Test
    public void testSequenceParameters() throws SQLException {
        String create_SQL = "CREATE SEQUENCE seq INCREMENT BY -2 MINVALUE -10 MAXVALUE -3 START WITH -4 CYCLE;";
        Statement stmt = conn.createStatement();
        stmt.execute(create_SQL);
        String nextval_SQL = "SELECT nextval('seq') FROM tbl;";
        String currval_SQL = "SELECT currval('seq') FROM tbl;";
        // First cycle
        for (int i = -4; i >= -10; i -= 2) {
            // nextval
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(nextval_SQL);
            boolean exists = rs.next();
            assert (exists);
            checkIntRow(rs, new String[]{"nextval(VARCHAR)"}, new int[]{i});
            assertNoMoreRows(rs);
            // currval
            stmt = conn.createStatement();
            rs = stmt.executeQuery(currval_SQL);
            exists = rs.next();
            assert (exists);
            checkIntRow(rs, new String[]{"currval(VARCHAR)"}, new int[]{i});
            assertNoMoreRows(rs);
        }
        // Second cycle
        for (int i = -3; i >= -10; i -= 2) {
            // nextval
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(nextval_SQL);
            boolean exists = rs.next();
            assert (exists);
            checkIntRow(rs, new String[]{"nextval(VARCHAR)"}, new int[]{i});
            assertNoMoreRows(rs);
            // currval
            stmt = conn.createStatement();
            rs = stmt.executeQuery(currval_SQL);
            exists = rs.next();
            assert (exists);
            checkIntRow(rs, new String[]{"currval(VARCHAR)"}, new int[]{i});
            assertNoMoreRows(rs);
        }
        String drop_SQL = "DROP SEQUENCE seq;";
        stmt = conn.createStatement();
        stmt.execute(drop_SQL);
    }
}
