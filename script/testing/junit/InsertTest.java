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
    private String s_sql = "SELECT * FROM tbl;";

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
    private void InitDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);
    }

    /**
     * Get columns, for follow on value checking
     */
    private void getResults() throws SQLException {
        Statement stmt = conn.createStatement();
        rs = stmt.executeQuery(s_sql);
    }

    /**
     * Init setup, only execute once before tests
     */
    @BeforeClass
    public static void Init() throws SQLException {

    }

    /**
     * Setup for each test, execute before each test
     * reconnect and setup default table
     */
    @Before
    public void Setup() throws SQLException {
        conn = makeDefaultConnection();
	    conn.setAutoCommit(true);
	    InitDatabase();
    }

    /**
     * Cleanup for each test, execute after each test
     * drop the default table
     */
    @After
    public void Teardown() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
    }

    /* --------------------------------------------
     * Insert statement tests
     * ---------------------------------------------
     */

    /**
     * 1 tuple insert, with no column specification.
     */
    @Test
    public void test_1Tuple_NCS() throws SQLException {
        String sql = "INSERT INTO tbl VALUES (1, 2, 3);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        getResults();
        rs.next();
	    checkRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 3});
	    assertNoMoreRows(rs);
    }

    /**
     * 1 tuple insert, with columns inserted in schema order.
     */
    @Test
    public void test_1Tuple_CS_1() throws SQLException {
        String sql = "INSERT INTO tbl (c1, c2, c3) VALUES (1, 2, 3);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        getResults();
        rs.next();
	    checkRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 3});
	    assertNoMoreRows(rs);
    }

    /**
     * 1 tuple insert, with columns inserted in different order from schema.
     * issue #729 wait to be fixed
     */ 
//    @Test
    public void test_1Tuple_CS_2() throws SQLException {
        String sql = "INSERT INTO tbl (c3, c1, c2) VALUES (3, 1, 2);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        getResults();
        rs.next();
	    checkRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 3});
	    assertNoMoreRows(rs);
    }

    /* 2 tuple inserts */

    /**
     * 2 tuple insert, with no column specification.
     */
    @Test
    public void test_2Tuple_NCS_1() throws SQLException {
        String sql = "INSERT INTO tbl VALUES (1, 2, 3), (11, 12, 13);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        getResults();
        rs.next();
	    checkRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 2, 3});

        rs.next();
	    checkRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {11, 12, 13});
	    assertNoMoreRows(rs);
    }

    /**
     * 2 tuple insert, with no column specification, with fewer than
     * schema columns
     * binding failed
     */
    // @Test
    public void test_2Tuple_NCS_2() throws SQLException {
        String sql = "INSERT INTO tbl VALUES (1), (11, 12);";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        getResults();
        rs.next();
	    checkRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {1, 0, 0});

	    rs.next();
	    checkRow(rs, new String [] {"c1", "c2", "c3"}, new int [] {11, 12, 0});
	    assertNoMoreRows(rs);
    }
    /**
     * CREATE TABLE with a qualified namespace doesn't work as expected
     * #706 fixed but also need #724 for select and drop
     */
    @Test
    public void test_CreateTableQualifiedNameSpace() throws SQLException {
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

        //@TODO drop the table foo.bar
    }

    /**
     * SELECT With Duplicate Columns Produces Zero Results
     * #720 fixed
     */
    @Test
    public void test_SelectDuplicateColumns() throws SQLException {
        String insert_SQL = "INSERT INTO tbl VALUES (1, 2, 3), (2, 3, 4);";
        Statement stmt = conn.createStatement();
        stmt.execute(insert_SQL);
        String select_SQL = "SELECT c1,c1 FROM tbl;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(select_SQL);
        rs.next();
	    checkRow(rs, new String [] {"c1", "c1"}, new int [] {1, 1});
        rs.next();
	    checkRow(rs, new String [] {"c1", "c1"}, new int [] {2, 2});
        assertNoMoreRows(rs);
    }

    /**
     * Invalid Implicit Casting of Integer Strings as Varchars
     * #733 fixed
     */
    @Test
    public void test_CastofIntegerString() throws SQLException {
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
        String drop_SQL = "DROP TABLE xxx01";
        stmt = conn.createStatement();
        stmt.execute(drop_SQL);
    }

}
