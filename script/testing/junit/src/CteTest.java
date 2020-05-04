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

public class CteTest extends TestUtility {
    private Connection conn;
    private ResultSet rs;
    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS COMPANY;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE COMPANY(\n" +
            "ID INT PRIMARY KEY NOT NULL,\n" +
            "NAME TEXT NOT NULL,\n" +
            "AGE INT NOT NULL,\n" +
            "ADDRESS CHAR(50),\n" +
            "SALARY REAL\n" +
            ");";

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);
        String insert_SQL_1 = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) VALUES (1, 'Paul', 32, 'California', 20000.00);";
        String insert_SQL_2 = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) VALUES (2, 'George', 21, 'NY', 10000.00);";
        stmt.execute(insert_SQL_1);
        stmt.execute(insert_SQL_2);
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
     * Cte tests
     * ---------------------------------------------
     */

    /*
    * Project Column Tests -
    * To check table schema and output schema coloids properly in tpl
    * */


    /**
     * Project columns from a cte that has a logical derived get query under it
     */
    @Test
    public void CteProjectColumnTest1() throws SQLException {
        String cte_statement = "WITH EMPLOYEE AS (SELECT ID,NAME,AGE FROM COMPANY) SELECT NAME FROM EMPLOYEE;";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 1);
        rs.next();
        assertEquals("Paul", rs.getString("NAME"));
        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertNoMoreRows(rs);
    }

    /**
     * Project columns from a cte that has a logical derived get query under it
     */
    @Test
    public void CteProjectColumnTest2() throws SQLException {
        String cte_statement = "WITH EMPLOYEE AS (SELECT ID,NAME,AGE FROM COMPANY) SELECT AGE FROM EMPLOYEE;";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 1);
        rs.next();
        assertEquals(32, rs.getInt("AGE"));
        rs.next();
        assertEquals(21, rs.getInt("AGE"));
        assertNoMoreRows(rs);
    }

    /**
     * Project columns from a cte that has a logical derived get query under it
     */
    @Test
    public void CteProjectColumnTest3() throws SQLException {
        String cte_statement = "WITH EMPLOYEE AS (SELECT ID,NAME,AGE FROM COMPANY) SELECT ID FROM EMPLOYEE;";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 1);
        rs.next();
        assertEquals(1, rs.getInt("ID"));
        rs.next();
        assertEquals(2, rs.getInt("ID"));
        assertNoMoreRows(rs);
    }


     /**
     * Sum aggregate inside cte
     */
    @Test
    public void CteSumAggregate() throws SQLException {
        String cte_statement = "WITH EMPLOYEE AS (SELECT SUM(AGE) AS SUMAGE FROM COMPANY) SELECT * FROM EMPLOYEE;";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 1);
        rs.next();
        assertEquals(53, rs.getInt("SUMAGE"));
        assertNoMoreRows(rs);
    }

    /**
     * Count aggregate inside cte
     */
    @Test
    public void CteCountAggregate() throws SQLException {
        String cte_statement = "WITH EMPLOYEE AS (SELECT COUNT(AGE) AS COUNTAGE FROM COMPANY) SELECT * FROM EMPLOYEE;";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 1);
        rs.next();
        assertEquals(2, rs.getInt("COUNTAGE"));
        assertNoMoreRows(rs);
    }

    /**
     * Join inside CTE query
     */
    @Test
    public void JoinInsideCte() throws SQLException {
        String cte_statement = "WITH EMPLOYEE AS (SELECT C1.NAME AS NAME," +
         "C2.AGE AS AGE FROM COMPANY AS C1, COMPANY AS C2) " +
          "SELECT * FROM EMPLOYEE; ";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 2);
        rs.next();
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(32, rs.getInt("AGE"));

        rs.next();
        assertEquals("George", rs.getString("NAME"));
        // **** NOTICE THE 32 HERE *******
        assertEquals(32, rs.getInt("AGE"));

        rs.next();
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(21, rs.getInt("AGE"));

        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertEquals(21, rs.getInt("AGE"));
        assertNoMoreRows(rs);
    }


    /**
     * Aggregate Expression with alias - This tests a special case inside the input column deriver for
     * Logical derived get with aliases that is below the cte node and we are referring a column with an alias
     * We get an aggreagate expression from the output expressions of the select query in the with clause
     * and use it convert it to a column value expression with the alias
     */
    @Test
    public void AggregateExpressionWithAlias() throws SQLException {
        String cte_statement = " WITH EMPLOYEE AS (SELECT MAX(AGE) AS MXAGE FROM COMPANY) " +
         "SELECT E2.NAME,E2.AGE FROM EMPLOYEE AS E1, " +
          "COMPANY AS E2 " +
           "WHERE E1.MXAGE = E2.AGE;\n";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 2);
        rs.next();
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(32, rs.getInt("AGE"));
        assertNoMoreRows(rs);
    }

    /**
     * Combination of expression with an alias and without an alias
     * This is a complicated query as we get an aggregate value expression with alias and a column value
     * expression without alias
     * This test makes sure that is handled correctly
     */
    @Test
    public void ExpressionWithAndWithoutAlias() throws SQLException {
            String cte_statement = " WITH EMPLOYEE AS (SELECT AGE+AGE AS SUMAGE,NAME FROM COMPANY) " +
         "SELECT E2.NAME,E1.SUMAGE FROM EMPLOYEE AS E1, " +
          "EMPLOYEE AS E2 WHERE E1.NAME = E2.NAME;";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 2);
        rs.next();
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(64, rs.getInt("SUMAGE"));

        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertEquals(42, rs.getInt("SUMAGE"));
        assertNoMoreRows(rs);

    }

    /**
     * Multiple Columns inside Aggregate Query
     */
    @Test
    public void MultipleColumnsInsideAggregateOfCte() throws SQLException {
            String cte_statement = "WITH EMPLOYEE AS (SELECT AGE+SALARY AS SUMAGE,NAME FROM COMPANY)" +
             "SELECT E2.NAME,E1.SUMAGE FROM EMPLOYEE AS E1," +
              "EMPLOYEE AS E2 WHERE E1.NAME = E2.NAME;\n";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 2);
        rs.next();
        double delta = 0.0001;
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(20032.0000, rs.getDouble("SUMAGE"), delta);

        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertEquals(10021.0000, rs.getDouble("SUMAGE"), delta);
        assertNoMoreRows(rs);
    }

    /**
     * Multiple Columns from Aggregate Query through CTE QUERY
     */
    @Test
    public void MultipleColumnsFromAggregateOfCte() throws SQLException {
            String cte_statement = "WITH EMPLOYEE AS (SELECT AGE,SALARY,NAME FROM COMPANY)" +
             "SELECT E2.NAME, (E1.AGE+E2.SALARY) AS SUMAGE FROM EMPLOYEE AS E1," +
              "EMPLOYEE AS E2 WHERE E1.NAME = E2.NAME;";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 2);
        rs.next();
        double delta = 0.0001;
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(20032.0000, rs.getDouble("SUMAGE"), delta);

        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertEquals(10021.0000, rs.getDouble("SUMAGE"), delta);
        assertNoMoreRows(rs);
    }

    /**
     * Multiple Columns from Aggregate Query through CTE QUERY with aliases
     * in some of the columns
     */
    @Test
    public void MultipleColumnsFromAggregateOfCteWithSomeAliasesInsideCte() throws SQLException {
            String cte_statement = "WITH EMPLOYEE AS (SELECT (AGE+AGE) AS SAGE,SALARY,NAME FROM COMPANY)" +
             "SELECT E2.NAME, (E1.SAGE+E2.SALARY) AS SUMAGE FROM EMPLOYEE AS E1," +
              "EMPLOYEE AS E2 WHERE E1.NAME = E2.NAME;";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 2);
        rs.next();
        double delta = 0.0001;
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(20064.0000, rs.getDouble("SUMAGE"), delta);

        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertEquals(10042.0000, rs.getDouble("SUMAGE"), delta);
        assertNoMoreRows(rs);
    }

    /**
     * Constant column with no alias - Constant column with no alias has to be handled separately
     * from the cases that we so far. We have to pass the expression to the child nodes.
     */
    @Test
    public void ConstantColumnWithNoAlias() throws SQLException {
            String cte_statement = " WITH EMPLOYEE AS (SELECT 1 ,NAME FROM COMPANY) " +
             "SELECT E2.NAME FROM EMPLOYEE AS E1, " +
              "EMPLOYEE AS E2 WHERE E1.NAME = E2.NAME;\n";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 1);
        rs.next();
        assertEquals("Paul", rs.getString("NAME"));
        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertNoMoreRows(rs);
    }

    /**
     * Constant column with alias - Constant column with alias has to be handled even separately
     * from the cases that we so far. We have to construct a column value expression for the constant column
     * with alias
     */
    @Test
    public void ConstantColumnWithAlias() throws SQLException {
            String cte_statement = "WITH EMPLOYEE AS (SELECT 21 AS RAWAGE FROM COMPANY)" +
             "SELECT E2.NAME FROM EMPLOYEE AS E1," +
              "COMPANY AS E2 " +
               "WHERE E1.RAWAGE = E2.AGE;\n";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 1);
        rs.next();
        assertEquals(columnsNumber, 1);
        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertNoMoreRows(rs);
    }

    /*Three Table Join with one table not from CTE*/
    @Test
    public void ThreeTableJoinOneWithCompany() throws SQLException {
        String cte_statement = "WITH EMPLOYEE AS (SELECT * FROM COMPANY) " +
         "SELECT E1.NAME,E2.AGE,E3.SALARY " +
          "FROM EMPLOYEE AS E1, " +
           "EMPLOYEE AS E2, " +
            "COMPANY AS E3;\n";
        Statement stmt = conn.createStatement();
        stmt = conn.createStatement();
        rs = stmt.executeQuery(cte_statement);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        assertEquals(columnsNumber, 3);
        double delta = 0.0001;

        rs.next();
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(32, rs.getInt("AGE"));
        assertEquals(20000.0000, rs.getDouble("SALARY"), delta);

        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertEquals(32, rs.getInt("AGE"));
        assertEquals(20000.0000, rs.getDouble("SALARY"), delta);

        rs.next();
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(21, rs.getInt("AGE"));
        assertEquals(20000.0000, rs.getDouble("SALARY"), delta);

        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertEquals(21, rs.getInt("AGE"));
        assertEquals(20000.0000, rs.getDouble("SALARY"), delta);

        rs.next();
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(32, rs.getInt("AGE"));
        assertEquals(10000.0000, rs.getDouble("SALARY"), delta);

        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertEquals(32, rs.getInt("AGE"));
        assertEquals(10000.0000, rs.getDouble("SALARY"), delta);

        rs.next();
        assertEquals("Paul", rs.getString("NAME"));
        assertEquals(21, rs.getInt("AGE"));
        assertEquals(10000.0000, rs.getDouble("SALARY"), delta);

        rs.next();
        assertEquals("George", rs.getString("NAME"));
        assertEquals(21, rs.getInt("AGE"));
        assertEquals(10000.0000, rs.getDouble("SALARY"), delta);
        assertNoMoreRows(rs);
    }

    /*Three Table Join with all tables from CTE*/
        @Test
        public void ThreeTableJoinAllCTE() throws SQLException {
            String cte_statement = "WITH EMPLOYEE AS (SELECT * FROM COMPANY) " +
             "SELECT E1.NAME,E2.AGE,E3.SALARY " +
              "FROM EMPLOYEE AS E1, " +
               "EMPLOYEE AS E2, " +
                "EMPLOYEE AS E3;\n";
            Statement stmt = conn.createStatement();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(cte_statement);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            assertEquals(columnsNumber, 3);
            double delta = 0.0001;

            rs.next();
            assertEquals("Paul", rs.getString("NAME"));
            assertEquals(32, rs.getInt("AGE"));
            assertEquals(20000.0000, rs.getDouble("SALARY"), delta);

            rs.next();
            assertEquals("George", rs.getString("NAME"));
            assertEquals(32, rs.getInt("AGE"));
            assertEquals(20000.0000, rs.getDouble("SALARY"), delta);

            rs.next();
            assertEquals("Paul", rs.getString("NAME"));
            assertEquals(21, rs.getInt("AGE"));
            assertEquals(20000.0000, rs.getDouble("SALARY"), delta);

            rs.next();
            assertEquals("George", rs.getString("NAME"));
            assertEquals(21, rs.getInt("AGE"));
            assertEquals(20000.0000, rs.getDouble("SALARY"), delta);

            rs.next();
            assertEquals("Paul", rs.getString("NAME"));
            assertEquals(32, rs.getInt("AGE"));
            assertEquals(10000.0000, rs.getDouble("SALARY"), delta);

            rs.next();
            assertEquals("George", rs.getString("NAME"));
            assertEquals(32, rs.getInt("AGE"));
            assertEquals(10000.0000, rs.getDouble("SALARY"), delta);

            rs.next();
            assertEquals("Paul", rs.getString("NAME"));
            assertEquals(21, rs.getInt("AGE"));
            assertEquals(10000.0000, rs.getDouble("SALARY"), delta);

            rs.next();
            assertEquals("George", rs.getString("NAME"));
            assertEquals(21, rs.getInt("AGE"));
            assertEquals(10000.0000, rs.getDouble("SALARY"), delta);
            assertNoMoreRows(rs);
        }

}
