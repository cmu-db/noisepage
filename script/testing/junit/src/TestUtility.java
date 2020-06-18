
/**
 * Base class (helper functions) for prepared statement tests
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import static org.junit.Assert.assertEquals;

public class TestUtility {
    public static Connection makeDefaultConnection() throws SQLException {
        return makeConnection("localhost", 15721, "terrier");
    }

    public static Connection makeConnection(String host, int port, String username) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("prepareThreshold", "0"); // suppress switchover to binary protocol

        // Set prepferQueryMode
        String preferQueryMode = System.getenv("TERRIER_QUERY_MODE");
        if (preferQueryMode == null || preferQueryMode.isEmpty()) {
            // Default as "simple" if TERRIER_QUERY_MODE is not specified
            preferQueryMode = "simple";
        }
        props.setProperty("preferQueryMode", preferQueryMode);

        // Set prepareThreshold if the prepferQueryMode is 'extended'
        if (preferQueryMode.equals("extended")) {
            String prepareThreshold = System.getenv("TERRIER_PREPARE_THRESHOLD");
            if (prepareThreshold != null && !prepareThreshold.isEmpty()) {
                props.setProperty("prepareThreshold", prepareThreshold);
            }
        }

        String url = String.format("jdbc:postgresql://%s:%d/", host, port);
        Connection conn = DriverManager.getConnection(url, props);
        return conn;
    }

    /**
     * Assert that we have consumed all the rows.
     *
     * @param rs resultset
     */
    public static void assertNoMoreRows(ResultSet rs) throws SQLException {
        int extra_rows = 0;
        while (rs.next()) {
            extra_rows++;
        }
        assertEquals(0, extra_rows);
    }

    /**
     * Check a single row of integer queried values against expected values
     *
     * @param rs       resultset, with cursor at the desired row
     * @param columns  column names
     * @param expected expected values of columns
     */
    public void checkIntRow(ResultSet rs, String[] columns, int[] expected) throws SQLException {
        assertEquals(columns.length, expected.length);
        for (int i = 0; i < columns.length; i++) {
            assertEquals(expected[i], rs.getInt(columns[i]));
        }
    }

    /**
     * Check a single row of real queried values against expected values
     *
     * @param rs       resultset, with cursor at the desired row
     * @param columns  column names
     * @param expected expected values of columns
     */
    public void checkDoubleRow(ResultSet rs, String[] columns, Double[] expected) throws SQLException {
        assertEquals(columns.length, expected.length);
        double delta = 0.0001;
        for (int i = 0; i < columns.length; i++) {
            Double val = (Double) rs.getObject(columns[i]);
            if (expected[i] == null) {
                assertEquals(expected[i], val);
            } else {
                assertEquals(expected[i], val, delta);
            }
        }
    }

    /**
     * Check a single row of real queried values against expected values
     *
     * @param rs       resultset, with cursor at the desired row
     * @param columns  column names
     * @param expected expected values of columns
     */
    public void checkStringRow(ResultSet rs, String[] columns, String[] expected) throws SQLException {
        assertEquals(columns.length, expected.length);
        for (int i = 0; i < columns.length; i++) {
            String val = (String) rs.getObject(columns[i]);
            if (expected[i] == null) {
                assertEquals(expected[i], val);
            } else {
                assertEquals(expected[i], val);
            }
        }
    }

    public static void DumpSQLException(SQLException ex) {
        System.err.println("Failed to execute test. Got " + ex.getClass().getSimpleName());
        System.err.println(" + Message:    " + ex.getMessage());
        System.err.println(" + Error Code: " + ex.getErrorCode());
        System.err.println(" + SQL State:  " + ex.getSQLState());
    }

    /**
     * Set column values.
     *
     * @param pstmt  prepared statement to receive values
     * @param values array of values
     */
    public void setValues(PreparedStatement pstmt, int[] values) throws SQLException {
        int col = 1;
        for (int i = 0; i < values.length; i++) {
            pstmt.setInt(col++, (int) values[i]);
        }
    }
}
