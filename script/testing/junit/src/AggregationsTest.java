/**
 * Basic validation of aggregation functionality.
 */

import java.sql.*;
import java.lang.*;
import org.junit.*;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationsTest extends TestUtility {
 private Connection conn;
 private ResultSet rs;

 @Before
 public void Setup() throws SQLException {
  conn = makeDefaultConnection();
  conn.setAutoCommit(true);
 }

 @After
 public void Teardown() throws SQLException {
  try {
    if (conn != null) {
        conn.close();
    }
  } catch (SQLException e) {
    DumpSQLException(e);
  }
 }

 /**
  * MaxAggregationIntTest
  */
 @Test
 public void test_MaxAggregationInt() throws SQLException {
  Statement stmt = conn.createStatement();
  
  stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count INT NOT NULL);");
  stmt.execute("INSERT INTO test (id, count) VALUES (0, 1), (1, 2);");

  stmt.execute("SELECT MAX(count) FROM test;");
  
  ResultSet rs = stmt.getResultSet();
  assertTrue(rs.next());
  assertEquals(rs.getInt(1), 2);

  stmt.execute("DROP TABLE test;");
 }

 /**
  * MaxAggregationFloatTest
  */
  @Test
  public void test_MaxAggregationFloat() throws SQLException {
   Statement stmt = conn.createStatement();
   
   stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count FLOAT NOT NULL);");
   stmt.execute("INSERT INTO test (id, count) VALUES (0, 1.0), (1, 2.0);");
 
   stmt.execute("SELECT MAX(count) FROM test;");
   
   ResultSet rs = stmt.getResultSet();
   assertTrue(rs.next());
   assertEquals(rs.getFloat(1), 2.0, 0D);
 
   stmt.execute("DROP TABLE test;");
  }

 /**
  * MaxAggregationDecimalTest
  */
  @Test
  public void test_MaxAggregationDecimal() throws SQLException {
   Statement stmt = conn.createStatement();
   
   stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count DECIMAL NOT NULL);");
   stmt.execute("INSERT INTO test (id, count) VALUES (0, 1), (1, 2);");
 
   stmt.execute("SELECT MAX(count) FROM test;");
   
   ResultSet rs = stmt.getResultSet();
   assertTrue(rs.next());
   assertEquals(rs.getFloat(1), 2.0, 0D);
 
   stmt.execute("DROP TABLE test;");
  }

 /**
  * MinAggregationIntTest
  */
  @Test
  public void test_MinAggregationInt() throws SQLException {
   Statement stmt = conn.createStatement();
   
   stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count INT NOT NULL);");
   stmt.execute("INSERT INTO test (id, count) VALUES (0, 1), (1, 2);");
 
   stmt.execute("SELECT MIN(count) FROM test;");
   
   ResultSet rs = stmt.getResultSet();
   assertTrue(rs.next());
   assertEquals(rs.getInt(1), 1);
 
   stmt.execute("DROP TABLE test;");
  }
 
  /**
   * MinAggregationFloatTest
   */
   @Test
   public void test_MinAggregationFloat() throws SQLException {
    Statement stmt = conn.createStatement();
    
    stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count FLOAT NOT NULL);");
    stmt.execute("INSERT INTO test (id, count) VALUES (0, 1.0), (1, 2.0);");
  
    stmt.execute("SELECT MIN(count) FROM test;");
    
    ResultSet rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertEquals(rs.getFloat(1), 1.0, 0D);
  
    stmt.execute("DROP TABLE test;");
   }
 
  /**
   * MinAggregationDecimalTest
   */
   @Test
   public void test_MinAggregationDecimal() throws SQLException {
    Statement stmt = conn.createStatement();
    
    stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count DECIMAL NOT NULL);");
    stmt.execute("INSERT INTO test (id, count) VALUES (0, 1), (1, 2);");
  
    stmt.execute("SELECT MIN(count) FROM test;");
    
    ResultSet rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertEquals(rs.getFloat(1), 1.0, 0D);
  
    stmt.execute("DROP TABLE test;");
   }

 /**
  * AvgAggregationIntTest
  */
  @Test
  public void test_AvgAggregationInt() throws SQLException {
   Statement stmt = conn.createStatement();
   
   stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count INT NOT NULL);");
   stmt.execute("INSERT INTO test (id, count) VALUES (0, 1), (1, 2);");
 
   stmt.execute("SELECT AVG(count) FROM test;");
   
   ResultSet rs = stmt.getResultSet();
   assertTrue(rs.next());
   assertEquals(rs.getFloat(1), 1.5, 0D);
 
   stmt.execute("DROP TABLE test;");
  }
 
  /**
   * AvgAggregationFloatTest
   */
   @Test
   public void test_AvgAggregationFloat() throws SQLException {
    Statement stmt = conn.createStatement();
    
    stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count FLOAT NOT NULL);");
    stmt.execute("INSERT INTO test (id, count) VALUES (0, 1.0), (1, 2.0);");
  
    stmt.execute("SELECT AVG(count) FROM test;");
    
    ResultSet rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertEquals(rs.getFloat(1), 1.5, 0D);
  
    stmt.execute("DROP TABLE test;");
   }
 
  /**
   * AvgAggregationDecimalTest
   */
   @Test
   public void test_AvgAggregationDecimal() throws SQLException {
    Statement stmt = conn.createStatement();
    
    stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count DECIMAL NOT NULL);");
    stmt.execute("INSERT INTO test (id, count) VALUES (0, 1), (1, 2);");
  
    stmt.execute("SELECT AVG(count) FROM test;");
    
    ResultSet rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertEquals(rs.getFloat(1), 1.5, 0D);
  
    stmt.execute("DROP TABLE test;");
   }

 /**
  * SumAggregationIntTest
  */
  @Test
  public void test_SumAggregationInt() throws SQLException {
   Statement stmt = conn.createStatement();
   
   stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count INT NOT NULL);");
   stmt.execute("INSERT INTO test (id, count) VALUES (0, 1), (1, 2);");
 
   stmt.execute("SELECT SUM(count) FROM test;");
   
   ResultSet rs = stmt.getResultSet();
   assertTrue(rs.next());
   assertEquals(rs.getInt(1), 3);
 
   stmt.execute("DROP TABLE test;");
  }
 
  /**
   * SumAggregationFloatTest
   */
   @Test
   public void test_SumAggregationFloat() throws SQLException {
    Statement stmt = conn.createStatement();
    
    stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count FLOAT NOT NULL);");
    stmt.execute("INSERT INTO test (id, count) VALUES (0, 1.0), (1, 2.0);");
  
    stmt.execute("SELECT SUM(count) FROM test;");
    
    ResultSet rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertEquals(rs.getFloat(1), 3.0, 0D);
  
    stmt.execute("DROP TABLE test;");
   }
 
  /**
   * SumAggregationDecimalTest
   */
   @Test
   public void test_SumAggregationDecimal() throws SQLException {
    Statement stmt = conn.createStatement();
    
    stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, count DECIMAL NOT NULL);");
    stmt.execute("INSERT INTO test (id, count) VALUES (0, 1), (1, 2);");
  
    stmt.execute("SELECT SUM(count) FROM test;");
    
    ResultSet rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertEquals(rs.getFloat(1), 3.0, 0D);
  
    stmt.execute("DROP TABLE test;");
   }
}