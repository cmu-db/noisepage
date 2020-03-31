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

public class InsertTest extends TestUtility {
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
     * 1 tuple insert, with no column specification.
     */
    @Test
    public void test1TupleNoColumn() throws SQLException {
        String sql = "INSERT INTO tbl VALUES (1, 2, 3);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        getResults();
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }

    /**
     * 1 tuple insert, with columns inserted in schema order.
     */
    @Test
    public void test1TupleColumnSchema() throws SQLException {
        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (1, 2, 3);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        getResults();
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }

    /**
     * 1 tuple insert, with columns inserted in different order from schema.
     * issue #729 wait to be fixed
     */ 
//    @Test
    public void test1TupleColumnDiffSchema() throws SQLException {
        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (3, 1, 2);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        getResults();
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 3});
        assertNoMoreRows(rs);
    }

    /* 2 tuple inserts */

    /**
     * 2 tuple insert, with no column specification.
     */
    @Test
    public void test2TupleNoCoulmn() throws SQLException {
        String sql = "INSERT INTO tbl VALUES (1, 2, 3), (11, 12, 13);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        getResults();
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 3});
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {11, 12, 13});
        assertNoMoreRows(rs);
    }

    /**
     * 2 tuple insert, with no column specification, with fewer than
     * schema columns
     * binding failed, wait to be fixed
     */
    // @Test
    public void test2TupleFewerColumn() throws SQLException {
        String sql = "INSERT INTO tbl VALUES (1), (11, 12);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        getResults();
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 0, 0});
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {11, 12, 0});
        assertNoMoreRows(rs);
    }

    /**
     * Test insertion of NULL values.
     * Fix #712.
     */
    @Test
    public void testInsertNull() throws SQLException {
        String createSQL = "CREATE TABLE xxx (col1 INT);";
        String insertSQL = "INSERT INTO xxx VALUES (NULL);";
        String selectSQL = "SELECT * FROM xxx;";
        String dropSQL = "DROP TABLE xxx;";

        Statement stmt = conn.createStatement();

        conn.setAutoCommit(false);
        stmt.addBatch(createSQL);
        stmt.addBatch(insertSQL);
        stmt.executeBatch();
        conn.commit();
        conn.setAutoCommit(true);

        stmt = conn.createStatement();
        rs = stmt.executeQuery(selectSQL);

        for (int i = 0; rs.next(); i++) {
            assertEquals(0, rs.getInt("col1"));
            assertEquals(true, rs.wasNull());
        }

        stmt = conn.createStatement();
        stmt.execute(dropSQL);
    }

    /**
     * CREATE TABLE with a qualified namespace doesn't work as expected
     * #706 fixed but also need #724 for select and drop
     */
    @Test
    public void testCreateTableQualifiedNameSpace() throws SQLException {
        String create_schema_SQL = "CREATE SCHEMA foo;";
        String create_table_SQL = "CREATE TABLE foo.bar (id integer);";

        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.addBatch(create_schema_SQL);
        stmt.addBatch(create_table_SQL);
        stmt.executeBatch();
        conn.commit();
        conn.setAutoCommit(true);

        String select_SQL = "SELECT * from pg_catalog.pg_class;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        while (rs.next()) {
            if (rs.getString("relname") == "bar") {
                assertEquals(1002,rs.getInt("relnamespace"));
            }
        }
        
        String drop_schema_SQL = "DROP SCHEMA foo;";
        stmt = conn.createStatement();
        stmt.execute(drop_schema_SQL);


    }

    /**
     * Invalid Implicit Casting of Integer Strings as Varchars
     * #733 fixed
     */
    @Test
    public void testCastofIntegerString() throws SQLException {
        conn.setAutoCommit(false);
        String create_SQL = "CREATE TABLE xxx01 (col0 VARCHAR(32) PRIMARY KEY, col1 VARCHAR(32));";
        Statement stmt = conn.createStatement();
        stmt.addBatch(create_SQL);
        int[] int_Array = new int[] {1319, 21995, 28037, 26984, 2762, 31763, 20359, 26022, 364, 831};
        for (int i = 0; i < int_Array.length; i++) {
            String insert_SQL = "INSERT INTO xxx01 VALUES ('" + Integer.toString(i) + 
                                "','" + int_Array[i] + "');";
            stmt.addBatch(insert_SQL);
        }
        stmt.executeBatch();    // group executions
        conn.commit();
        conn.setAutoCommit(true);
        String select_SQL = "SELECT * FROM xxx01;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        int i = 0;
        while (rs.next()) {
            assertEquals(int_Array[i],rs.getInt("col1"));
            i++;
        }
        String drop_SQL = "DROP TABLE xxx01;";
        stmt = conn.createStatement();
        stmt.execute(drop_SQL);
    }

    /**
     * Support default value expressions
     * #718 fixed
     */
    @Test
    public void testDefaultValueInsert () throws SQLException {
        String create_table_SQL = "CREATE TABLE xxx (id integer, val integer DEFAULT 123);";
        String insert_into_table_SQL = "INSERT INTO xxx VALUES (1, DEFAULT);";
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.addBatch(create_table_SQL);
        stmt.addBatch(insert_into_table_SQL);
        stmt.executeBatch();
        conn.commit();
        conn.setAutoCommit(true);

        String select_SQL = "SELECT * from xxx;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"id", "val"}, new int [] {1, 123});
        assertNoMoreRows(rs);

        String drop_SQL = "DROP TABLE xxx;";
        stmt = conn.createStatement();
        stmt.execute(drop_SQL);
    }

    /**
     * Support out of order inserts
     * #729 fixed
     */
    @Test
    public void testOutOfOrderInserts () throws SQLException {
        String create_table_SQL = "CREATE TABLE xxx (c1 integer, c2 integer, c3 integer);";
        String insert_into_table_SQL = "INSERT INTO xxx (c2, c1, c3) VALUES (2, 3, 4);";
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.addBatch(create_table_SQL);
        stmt.addBatch(insert_into_table_SQL);
        stmt.executeBatch();
        conn.commit();
        conn.setAutoCommit(true);

        String select_SQL = "SELECT * from xxx;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {3, 2, 4});
        assertNoMoreRows(rs);

        String drop_SQL = "DROP TABLE xxx;";
        stmt = conn.createStatement();
        stmt.execute(drop_SQL);
    }

    /**
     * Support out of order inserts with defaults
     * #718 fixed
     */
    @Test
    public void testOutOfOrderInsertsWithDefaults () throws SQLException {
        String create_table_SQL = "CREATE TABLE xxx (c1 integer, c2 integer, c3 integer DEFAULT 34);";
        String insert_into_table_SQL = "INSERT INTO xxx (c2, c1) VALUES (2, 3);";
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.addBatch(create_table_SQL);
        stmt.addBatch(insert_into_table_SQL);
        stmt.executeBatch();
        conn.commit();
        conn.setAutoCommit(true);

        String select_SQL = "SELECT * from xxx;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
        checkIntRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {3, 2, 34});
        assertNoMoreRows(rs);

        String drop_SQL = "DROP TABLE xxx;";
        stmt = conn.createStatement();
        stmt.execute(drop_SQL);
    }

    /**
     * Test inserting default expressions that are not INTEGER type.
     * #831 BinderSherpa
     */
    @Test
    public void testDefaultValueInsertBigInt() throws SQLException {
        String createSQL = "CREATE TABLE xxx (c1 integer, c2 integer, c3 bigint DEFAULT 4398046511104);";
        String insertSQL = "INSERT INTO xxx (c1, c2) VALUES (1, 2);";
        String selectSQL = "SELECT * from xxx;";
        String dropSQL = "DROP TABLE xxx;";

        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.addBatch(createSQL);
        stmt.addBatch(insertSQL);
        stmt.executeBatch();
        conn.commit();
        conn.setAutoCommit(true);

        stmt = conn.createStatement();
        rs = stmt.executeQuery(selectSQL);
        rs.next();
        assertEquals(1, rs.getInt("c1"));
        assertEquals(2, rs.getInt("c2"));
        assertEquals(4398046511104L, rs.getLong("c3"));
        rs.next();

        assertNoMoreRows(rs);

        stmt = conn.createStatement();
        stmt.execute(dropSQL);
    }

    /**
     * Test inserting cast expressions for BIGINT and TIMESTAMP.
     * #831 BinderSherpa
     */
    @Test
    public void testInsertBigIntCastTimestampCast() throws SQLException {
        String ts1 = "2020-01-02 12:23:34.56789";
        String createSQL = "CREATE TABLE xxx (c1 bigint, c2 timestamp);";
        String insertSQL = "INSERT INTO xxx (c1, c2) VALUES ('123'::bigint, '" + ts1 + "::timestamp');";
        String selectSQL = "SELECT * from xxx;";
        String dropSQL = "DROP TABLE xxx;";

        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.addBatch(createSQL);
        stmt.addBatch(insertSQL);
        stmt.executeBatch();
        conn.commit();
        conn.setAutoCommit(true);

        stmt = conn.createStatement();
        rs = stmt.executeQuery(selectSQL);

        rs.next();
        assertEquals(123, rs.getLong("c1"));
        assertEquals(ts1, rs.getTimestamp("c2").toString());
        rs.next();

        assertNoMoreRows(rs);

        stmt = conn.createStatement();
        stmt.execute(dropSQL);
    }

}
