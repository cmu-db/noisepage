/**
 * Insert statement tests.
 */

import java.sql.*;
import org.junit.*;
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

    @BeforeClass
    public static void Init() throws SQLException {

    }

    @Before
    public void Setup() throws SQLException {
        conn = makeDefaultConnection();
	    conn.setAutoCommit(true);
	    InitDatabase();
    }

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
    /*
     * issue #706 fixed. Also need issue #724 for drop
     * CREATE TABLE with a qualified namespace doesn't work as expected
     */
    // @Test
    public void test_issue_706() throws SQLException {
        String createSchemaSQL = "create schema foo;";
        String createTableSQL = "create table foo.bar (id integer);";
        String selectSQL = "SELECT * from pg_catalog.pg_class;";
    }

    /*
     * issue #720 fixed
     */
    @Test
    public void test_issue_720() throws SQLException {
        String insertSQL = "INSERT INTO tbl VALUES (1, 2, 3), (2, 3, 4);";
        Statement stmt = conn.createStatement();
        stmt.execute(insertSQL);
        String selectSQL = "SELECT c1,c1 FROM tbl;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(selectSQL);
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c1"},
		 new int [] {1, 1});
        rs.next();
	checkRow(rs,
		 new String [] {"c1", "c1"},
		 new int [] {2, 2});
        assertNoMoreRows(rs);
    }

    /**
     * issue #733 fixed
     */
    @Test
    public void test_issue_733() throws SQLException {
        conn.setAutoCommit(false);
        String createSQL = "CREATE TABLE xxx01 (col0 VARCHAR(32) PRIMARY KEY, col1 VARCHAR(32));";
        Statement stmt = conn.createStatement();
        stmt.addBatch(createSQL);
        int[] intArray = new int[] {1319, 21995, 28037, 26984, 2762, 31763, 20359, 26022, 364, 831};
        for (int i = 0; i < intArray.length; i++) {
            String insertSQL = "INSERT INTO xxx01 VALUES ('" + Integer.toString(i) + 
                                "','" + intArray[i] + "');";
            stmt.addBatch(insertSQL);
        }
        stmt.executeBatch();
        conn.commit();
        conn.setAutoCommit(true);
        String selectSQL = "SELECT * FROM xxx01;";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(selectSQL);
        int i = 0;
        while (rs.next()) {
            assertEquals(intArray[i],rs.getInt("col1"));
            i++;
        }
        String dropSQL = "DROP TABLE xxx01";
        stmt = conn.createStatement();
        stmt.execute(dropSQL);
    }


}
