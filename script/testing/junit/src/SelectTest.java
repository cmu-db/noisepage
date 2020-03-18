/**
 * Insert statement tests.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class SelectTest extends TestUtility {
    private Connection conn;
    private ResultSet rs;
    private String S_SQL = "SELECT * FROM tbl;";

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS tbl;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE tbl (" +
                    "c1 int NOT NULL PRIMARY KEY, " +
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
     * Select statement tests
     * ---------------------------------------------
     */

    /**
     * SELECT With Duplicate Columns Produces Zero Results
     * #720 fixed
     */
    @Test
    public void testSelectDuplicateColumns() throws SQLException {
        String insert_SQL = "INSERT INTO tbl VALUES (1, 2, 3), (2, 3, 4);";
        Statement stmt = conn.createStatement();
        stmt.execute(insert_SQL);
        String select_SQL = "SELECT c1,c1 FROM tbl;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c1"}, new int [] {1, 1});
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c1"}, new int [] {2, 2});
        assertNoMoreRows(rs);
    }

    /**
     * SELECT With Alias
     * #795 fixed
     */
    @Test
    public void testSelectAlias() throws SQLException {
        String insert_SQL = "INSERT INTO tbl VALUES (1, 2, 3), (2, 3, 4);";
        Statement stmt = conn.createStatement();
        stmt.execute(insert_SQL);
        /* simple alias */
        String select_SQL = "SELECT c1 as new_name FROM tbl;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"new_name"}, new int [] {1});
        rs.next();
        checkIntRow(rs, new String [] {"new_name"}, new int [] {2});
        assertNoMoreRows(rs);

        /* constant column name */
        select_SQL = "SELECT 1 as new_name FROM tbl;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"new_name"}, new int [] {1});
        rs.next();
        checkIntRow(rs, new String [] {"new_name"}, new int [] {1});
        assertNoMoreRows(rs);

        /* alternate column name */
        select_SQL = "SELECT c1 as c2, c2 as c1 FROM tbl;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"c2", "c1"}, new int [] {1, 2});
        rs.next();
        checkIntRow(rs, new String [] {"c2", "c1"}, new int [] {2, 3});
        assertNoMoreRows(rs);

        /* Wrong count, wait for Having fix */
        select_SQL = "SELECT COUNT(*) AS cnt FROM tbl HAVING COUNT(*) > 10;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        assertNoMoreRows(rs);

        /* self name */
        select_SQL = "SELECT c1 as c1, c2 as c2 FROM tbl;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2"}, new int [] {1, 2});
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2"}, new int [] {2, 3});
        assertNoMoreRows(rs);

        /* confusing name */
        select_SQL = "SELECT c1, c2 as c1 FROM tbl;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        ResultSetMetaData metaData = rs.getMetaData();
        assertEquals(metaData.getColumnLabel(1), "c1");
        assertEquals(metaData.getColumnLabel(2), "c1");
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(3, rs.getInt(2));
        assertNoMoreRows(rs);

        /* tpcc select, broken test */
        insert_SQL = "INSERT INTO tbl VALUES (3, 2, 10), (4, 3, 20);";  //duplicate c2
        stmt = conn.createStatement();
        stmt.execute(insert_SQL);

        select_SQL = "SELECT COUNT(DISTINCT c2) AS STOCK_COUNT FROM tbl;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"stock_count"}, new int [] {2});
        assertNoMoreRows(rs);
    }

    /**
     * SELECT with arithmetic on integer and reals.
     */
    @Test
    public void testSelectAddIntegerAndReal() throws SQLException {
      String drop_SQL = "DROP TABLE IF EXISTS tbl;";
      String create_SQL = "CREATE TABLE tbl (a int, b float);";
      String insert_SQL = "INSERT INTO tbl VALUES (1, 1.37);";
      String select_SQL = "SELECT a + b AS AB FROM tbl;";
      Statement stmt = conn.createStatement();
      stmt.execute(drop_SQL);
      stmt = conn.createStatement();
      stmt.execute(create_SQL);
      stmt = conn.createStatement();
      stmt.execute(insert_SQL);
      stmt = conn.createStatement();
      rs = stmt.executeQuery(select_SQL);
      rs.next();
      checkDoubleRow(rs, new String [] {"AB"}, new double[] {2.37});
      assertNoMoreRows(rs);
      stmt = conn.createStatement();
      stmt.execute(drop_SQL);
    }
}
