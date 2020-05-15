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
    private Connection conn2;
    private Connection conn3;
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
        conn2 = makeDefaultConnection();
        conn2.setAutoCommit(true);
        conn3 = makeDefaultConnection();
        conn3.setAutoCommit(false);
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
     * Sequence tests
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

    /**
     * Tests sequence with multiple connections
     */
    @Test
    public void testSequenceMultipleConnections() throws SQLException {
        String create_SQL = "CREATE SEQUENCE seq;";
        Statement stmt = conn.createStatement();
        stmt.execute(create_SQL);
        String nextval_SQL = "SELECT nextval('seq') FROM tbl;";
        String currval_SQL = "SELECT currval('seq') FROM tbl;";
        for (int i = 1; i < 10; i += 2) {
            // nextval by the first connection
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(nextval_SQL);
            boolean exists = rs.next();
            assert (exists);
            checkIntRow(rs, new String[]{"nextval(VARCHAR)"}, new int[]{i});
            assertNoMoreRows(rs);
            // nextval by the second connection
            stmt = conn2.createStatement();
            rs = stmt.executeQuery(nextval_SQL);
            exists = rs.next();
            assert (exists);
            checkIntRow(rs, new String[]{"nextval(VARCHAR)"}, new int[]{i + 1});
            assertNoMoreRows(rs);
            // currval by the first connection
            stmt = conn.createStatement();
            rs = stmt.executeQuery(currval_SQL);
            exists = rs.next();
            assert (exists);
            checkIntRow(rs, new String[]{"currval(VARCHAR)"}, new int[]{i});
            assertNoMoreRows(rs);
            // currval by the second connection
            stmt = conn2.createStatement();
            rs = stmt.executeQuery(currval_SQL);
            exists = rs.next();
            assert (exists);
            checkIntRow(rs, new String[]{"currval(VARCHAR)"}, new int[]{i + 1});
            assertNoMoreRows(rs);
        }
        String drop_SQL = "DROP SEQUENCE seq;";
        stmt = conn.createStatement();
        stmt.execute(drop_SQL);
    }

    /**
     * Tests sequence's mini transactions
     */
    @Test
    public void testSequenceMiniTransaction() throws SQLException {
        // Check if rollback and commit have correct behavior
        String create_SQL = "CREATE SEQUENCE seq;";
        Statement stmt = conn3.createStatement();
        stmt.execute(create_SQL);
        conn3.rollback();
        stmt = conn3.createStatement();
        stmt.execute(create_SQL);
        conn3.commit();

        // Call nextval and commit
        String nextval_SQL = "SELECT nextval('seq') FROM tbl;";
        stmt = conn3.createStatement();
        ResultSet rs = stmt.executeQuery(nextval_SQL);
        boolean exists = rs.next();
        assert (exists);
        checkIntRow(rs, new String[]{"nextval(VARCHAR)"}, new int[]{1});
        assertNoMoreRows(rs);
        conn3.commit();

        // Call nextval and rollback
        stmt = conn3.createStatement();
        rs = stmt.executeQuery(nextval_SQL);
        exists = rs.next();
        assert (exists);
        checkIntRow(rs, new String[]{"nextval(VARCHAR)"}, new int[]{2});
        assertNoMoreRows(rs);
        conn3.rollback();

        // currval should return the latest value
        String currval_SQL = "SELECT currval('seq') FROM tbl;";
        stmt = conn3.createStatement();
        rs = stmt.executeQuery(currval_SQL);
        exists = rs.next();
        assert (exists);
        checkIntRow(rs, new String[]{"currval(VARCHAR)"}, new int[]{2});
        assertNoMoreRows(rs);
        conn3.commit();

        // nextval should return a new value
        stmt = conn3.createStatement();
        rs = stmt.executeQuery(nextval_SQL);
        exists = rs.next();
        assert (exists);
        checkIntRow(rs, new String[]{"nextval(VARCHAR)"}, new int[]{3});
        assertNoMoreRows(rs);
        conn3.commit();

        String drop_SQL = "DROP SEQUENCE seq;";
        stmt = conn.createStatement();
        stmt.execute(drop_SQL);
        conn3.commit();
    }
}
