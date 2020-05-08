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

public class NestedQueryTest extends TestUtility {
    private Connection conn;
    private ResultSet rs;

    private static final String SQL_DROP_SHIPMENT =
            "DROP TABLE IF EXISTS shipment;";
    private static final String SQL_DROP_SUPPLIER =
            "DROP TABLE IF EXISTS supplier;";
    private static final String SQL_DROP_PART =
            "DROP TABLE IF EXISTS part;";

    private static final String SQL_CREATE_SHIPMENT =
            "CREATE TABLE shipment (" +
                    "sno int, " +
                    "pno int," +
                    "qty int);";
    private static final String SQL_CREATE_SUPPLIER =
            "CREATE TABLE supplier (" +
                    "sno int, " +
                    "sloc int," +
                    "subdget int);";
    private static final String SQL_CREATE_PART =
            "CREATE TABLE part (" +
                    "pno int, " +
                    "price int);";

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_SHIPMENT);
        stmt.execute(SQL_DROP_SUPPLIER);
        stmt.execute(SQL_DROP_PART);
        stmt.execute(SQL_CREATE_SHIPMENT);
        stmt.execute(SQL_CREATE_SUPPLIER);
        stmt.execute(SQL_CREATE_PART);
    }

    /**
     * Setup for each test, make the default connection
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
            stmt.execute(SQL_DROP_SHIPMENT);
            stmt.execute(SQL_DROP_SUPPLIER);
            stmt.execute(SQL_DROP_PART);
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

    /*
     * ---------------------------------------------
     * Nested query tests
     * ---------------------------------------------
     */
    /**
     * Test TypeA, max
     */
    @Test
    public void test1TypeAMaxSimple() throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = "INSERT INTO shipment VALUES (2, 2, 2);";
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (3, 3, 3);";
        stmt.execute(sql);
        sql = "INSERT INTO part VALUES (1, 30);";
        stmt.execute(sql);
        sql = "INSERT INTO part VALUES (2, 30);";
        stmt.execute(sql);
        String select_SQL = "SELECT sno FROM shipment WHERE pno = (SELECT MAX(pno) FROM part WHERE price = 30);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"sno"}, new int [] {2});
        assertNoMoreRows(rs);
    }

    /**
     * Test TypeA, max, multiple results
     */
    @Test
    public void test2TypeAMaxMulti() throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = "INSERT INTO shipment VALUES (2, 2, 2);";
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (3, 2, 3);";
        stmt.execute(sql);
        sql = "INSERT INTO part VALUES (1, 30);";
        stmt.execute(sql);
        sql = "INSERT INTO part VALUES (2, 30);";
        stmt.execute(sql);
        String select_SQL = "SELECT sno FROM shipment WHERE pno = (SELECT MAX(pno) FROM part WHERE price = 30);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"sno"}, new int [] {2});
        rs.next();
        checkIntRow(rs, new String [] {"sno"}, new int [] {3});
        assertNoMoreRows(rs);
    }

    /**
     * Test TypeA, max, not equal
     */
    @Test
    public void test2TypeAMaxNot() throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = "INSERT INTO shipment VALUES (2, 2, 2);";
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (3, 2, 3);";
        stmt.execute(sql);
        sql = "INSERT INTO part VALUES (1, 30);";
        stmt.execute(sql);
        sql = "INSERT INTO part VALUES (2, 30);";
        stmt.execute(sql);
        String select_SQL = "SELECT sno FROM shipment WHERE pno != (SELECT MAX(pno) FROM part WHERE price = 30);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"sno"}, new int [] {1});
        assertNoMoreRows(rs);
    }

    /**
     * Test TypeA, avg
     */
    @Test
    public void test2TypeAAvgSimple() throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = "INSERT INTO shipment VALUES (2, 2, 2);";
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (3, 3, 3);";
        stmt.execute(sql);
        sql = "INSERT INTO part VALUES (1, 30);";
        stmt.execute(sql);
        sql = "INSERT INTO part VALUES (3, 30);";
        stmt.execute(sql);
        String select_SQL = "SELECT sno FROM shipment WHERE pno = (SELECT AVG(pno) FROM part WHERE price = 30);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"sno"}, new int [] {2});
        assertNoMoreRows(rs);
    }
     /**
     * Test TypeA, avg, multiple results
     */
    @Test
    public void test2TypeAAvgMultiple() throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = "INSERT INTO shipment VALUES (2, 2, 2);";
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (3, 2, 3);";
        stmt.execute(sql);
        sql = "INSERT INTO part VALUES (1, 30);";
        stmt.execute(sql);
        sql = "INSERT INTO part VALUES (3, 30);";
        stmt.execute(sql);
        String select_SQL = "SELECT sno FROM shipment WHERE pno = (SELECT AVG(pno) FROM part WHERE price = 30);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"sno"}, new int [] {2});
        rs.next();
        checkIntRow(rs, new String [] {"sno"}, new int [] {3});
        assertNoMoreRows(rs);

    }

    /**
     * Test TypeN
     */
    @Test
    public void test1TypeNSimple() throws SQLException {
        String sql = "INSERT INTO shipment VALUES (2, 2, 1);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (1, 2, 1);";
        stmt.execute(sql);
        String select_SQL = "SELECT pno FROM shipment WHERE sno in (SELECT sno FROM supplier);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"pno"}, new int [] {1});
        assertNoMoreRows(rs);
    }

    /**
     * Test TypeN, No duplicates due to COMPARE_IN operation
     */
    @Test
    public void test2TypeNNoDuplicates() throws SQLException {
        String sql = "INSERT INTO shipment VALUES (2, 2, 1);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (1, 2, 1);";
        stmt.execute(sql);
        String select_SQL = "SELECT pno FROM shipment WHERE sno in (SELECT sno FROM supplier);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"pno"}, new int [] {1});
        assertNoMoreRows(rs);
    }

    /**
     * Test TypeN, Multiple nested layers
     */
    @Test
    public void test3TypeNMulti() throws SQLException {
        String sql = "INSERT INTO shipment VALUES (2, 2, 1);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (2, 2, 2);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (1, 2, 1);";
        stmt.execute(sql);
        String select_SQL = "SELECT pno FROM shipment WHERE sno in (SELECT sno FROM supplier WHERE subdget in (SELECT qty FROM shipment));";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"pno"}, new int [] {2});
        rs.next();
        checkIntRow(rs, new String [] {"pno"}, new int [] {1});
        assertNoMoreRows(rs);
    }

    /**
     * Test TypeJ
     */
    @Test
    public void test1TypeJ() throws SQLException {
        String sql = "INSERT INTO shipment VALUES (2, 2, 1);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (1, 1, 2);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (1, 2, 2);";
        stmt.execute(sql);
        String select_SQL = "SELECT pno FROM shipment WHERE sno in (SELECT sno FROM supplier WHERE subdget = qty);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"pno"}, new int [] {1});
        assertNoMoreRows(rs);
    }

    /**
     * Test TypeJA, less than max
     */
    @Test
    public void test1TypeJA() throws SQLException {
        String sql = "INSERT INTO shipment VALUES (1, 2, 1);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (2, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (2, 1, 1);";
        stmt.execute(sql);
        String select_SQL = "SELECT pno FROM shipment WHERE sno < (SELECT max(sno) FROM supplier WHERE subdget = qty);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"pno"}, new int [] {2});
        assertNoMoreRows(rs);
    }

    /**
     * Test TypeJA, equal to max
     */
    @Test
    public void test2TypeJA() throws SQLException {
        String sql = "INSERT INTO shipment VALUES (2, 2, 2);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (1, 2, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (2, 1, 1);";
        stmt.execute(sql);
        String select_SQL = "SELECT pno FROM shipment WHERE sno = (SELECT min(sno) FROM supplier WHERE subdget = qty);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"pno"}, new int [] {2});
        assertNoMoreRows(rs);
    }

    /**
     * Test TypeJA, greater than avg
     */
    @Test
    public void test3TypeJA() throws SQLException {
        String sql = "INSERT INTO shipment VALUES (2, 2, 2);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        sql = "INSERT INTO shipment VALUES (2, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (1, 1, 1);";
        stmt.execute(sql);
        sql = "INSERT INTO supplier VALUES (2, 1, 1);";
        stmt.execute(sql);
        String select_SQL = "SELECT pno FROM shipment WHERE sno > (SELECT avg(sno) FROM supplier WHERE subdget = qty);";
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"pno"}, new int [] {1});
        assertNoMoreRows(rs);
    }
}
