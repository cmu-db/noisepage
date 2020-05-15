/**
 * Insert statement tests.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Types.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class FunctionsTest extends TestUtility {
    private Connection conn;
    private ResultSet rs;
    private String S_SQL = "SELECT * FROM data;";

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS data;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE data (" +
                    "int_val INT, " +
                    "double_val DECIMAL," +
                    "str_i_val VARCHAR(32)," + // Integer as string
                    "str_a_val VARCHAR(32)," + // Alpha string
//                     "bool_val BOOL," +
                    "is_null INT)";

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);

        String sql = "INSERT INTO data (" +
                     "int_val, double_val, str_i_val, str_a_val, " +
//                      "bool_val, " + 
                     "is_null " +
                     ") VALUES (?, ?, ?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);

        // Non-Null Values
        int idx = 1;
        pstmt.setInt(idx++, 123);
        pstmt.setDouble(idx++, 12.34);
        pstmt.setString(idx++, "123456");
        pstmt.setString(idx++, "AbCdEf");
//         pstmt.setBoolean(idx++, true);
        pstmt.setInt(idx++, 0);
//         pstmt.setBoolean(idx++, false);
        pstmt.addBatch();
        
        // Null Values
        idx = 1;
        pstmt.setNull(idx++, java.sql.Types.INTEGER);
        pstmt.setNull(idx++, java.sql.Types.DOUBLE);
        pstmt.setNull(idx++, java.sql.Types.VARCHAR);
        pstmt.setNull(idx++, java.sql.Types.VARCHAR);
//         pstmt.setNull(idx++, java.sql.Types.BOOLEAN);
        pstmt.setInt(idx++, 1);
//         pstmt.setBoolean(idx++, true);
        pstmt.addBatch();
        
        pstmt.executeBatch();
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

    private void checkDoubleFunc(String func_name, String col_name, boolean is_null, Double expected) throws SQLException {
        String sql = String.format("SELECT %s(%s) AS result FROM data WHERE is_null = %s",
                                   func_name, col_name, (is_null ? 1 : 0));
                                   
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean exists = rs.next();
        assert(exists);
        if (is_null) {
            checkDoubleRow(rs, new String[]{"result"}, new Double[]{null});
        } else {
            checkDoubleRow(rs, new String[]{"result"}, new Double[]{expected});
        }
        assertNoMoreRows(rs);
    }
    
    private void checkStringFunc(String func_name, String col_name, boolean is_null, String expected) throws SQLException {
        String sql = String.format("SELECT %s(%s) AS result FROM data WHERE is_null = %s",
                                   func_name, col_name, (is_null ? 1 : 0));
        
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean exists = rs.next();
        assert(exists);
        if (is_null) {
            checkStringRow(rs, new String[]{"result"}, new String[]{null});
        } else {
            checkStringRow(rs, new String[]{"result"}, new String[]{expected});
        }
        assertNoMoreRows(rs);
    }
     
    /**
     * Tests usage of trig udf functions
     * #744 test
     */
    @Test
    public void testCos() throws SQLException {
        checkDoubleFunc("cos", "double_val", false, 0.974487);
        checkDoubleFunc("cos", "double_val", true, null);
    }
    @Test
    public void testSin() throws SQLException {
        checkDoubleFunc("sin", "double_val", false, -0.224442);
        checkDoubleFunc("sin", "double_val", true, null);
    }
    @Test
    public void testTan() throws SQLException {
        checkDoubleFunc("tan", "double_val", false, -0.230318);
        checkDoubleFunc("tan", "double_val", true, null);
    }
    
    /**
     * String Functions
     */
    @Test
    public void testLower() throws SQLException {
        checkStringFunc("lower", "str_a_val", false, "abcdef");
        checkStringFunc("lower", "str_a_val", true, null);
    }

}
