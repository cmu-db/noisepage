/**
 * Matches the basic tests in traffic_cop_test (this uses Extended Query protocol, that uses Simple Query)
 */

import java.sql.*;
import org.junit.*;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class TrafficCopTest extends TestUtility {
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
  * EmptyStatementTest
  */
 @Test
 public void test_EmptyStatement() throws SQLException {
  Statement stmt = conn.createStatement();
  stmt.execute("");
 }

 /**
   * DisconnectAbortTest
   */
  @Test
  public void test_DisconnectAbort() throws SQLException {

   // create another connection that will take the write lock on a tuple, forcing an abort on the default connection's
   // createStatement
   Connection second_conn = makeDefaultConnection();
   second_conn.setAutoCommit(true);

   Statement stmt = second_conn.createStatement();
   stmt.execute("CREATE TABLE FOO (ID INT);");
   stmt.execute("INSERT INTO FOO VALUES (1),(2),(3);");

   // begin explicit txn and take the write lock on a tuple
   stmt.execute("BEGIN;");
   stmt.execute("UPDATE FOO SET ID = 4 WHERE ID = 3;");
   assertEquals(stmt.getUpdateCount(), 1);

   Statement second_stmt = conn.createStatement();
   try {
      // another statement tries to update the same tuple and will fail
      second_stmt.execute("UPDATE FOO SET ID = 5 WHERE ID = 3;");
      fail();
     } catch (SQLException ex) {
      assertEquals(ex.getMessage(), "Query failed.");
     }
   // close the second connection, forcing the explicit txn that has the write lock to abort
   second_conn.close();

   // another statement can now acquire the write lock on the tuple
   Statement third_stmt = conn.createStatement();
   third_stmt.execute("UPDATE FOO SET ID = 5 WHERE ID = 3;");
   assertEquals(third_stmt.getUpdateCount(), 1);
   third_stmt.execute("DROP TABLE FOO;");


  }

 /**
  * BadParseTest
  */
 @Test
 public void test_BadParse() {
  try {
   Statement stmt = conn.createStatement();
   stmt.execute("INSTERT INTO FOO VALUES (1,1);");
   fail();
  } catch (SQLException ex) {
   assertEquals(ex.getMessage(), "ERROR:  syntax error");
  }
 }

 /**
  * BadBindingTest
  */
 @Test
 public void test_BadBinding() {
  try {
   Statement stmt = conn.createStatement();
   stmt.execute("INSERT INTO FOO VALUES (1,1);");
   fail();
  } catch (SQLException ex) {
   assertEquals(ex.getMessage(), "ERROR:  binding failed");
  }
 }


 /**
  * DDL Statements
  */
 @Test
 public void test_DDLStatements() {
 try {
  Statement stmt = conn.createStatement();
  stmt.execute("CREATE TABLE FOO (id INT);"); // will succeed
  try {
   stmt.execute("CREATE TABLE FOO (id INT);"); // fail for duplicate table name, make sure it re-bound the potentially cached statement
   fail();
  } catch (SQLException ex) {
   assertEquals(ex.getMessage(), "ERROR:  binding failed");
  }
  stmt.execute("DROP TABLE FOO;"); // will succeed
  stmt.execute("DROP TABLE IF EXISTS FOO;");  // will succeed due to IF EXISTS
  try {
   stmt.execute("DROP TABLE FOO;");  // fail for table not existing anymore, make sure it re-bound the potentially cached statement
   fail();
  } catch (SQLException ex) {
   assertEquals(ex.getMessage(), "ERROR:  binding failed");
  }
  stmt.execute("CREATE TABLE FOO (id INT);"); // will succeed, make sure it re-bound the potentially cached statement
  try {
   stmt.execute("CREATE TABLE FOO (id INT);"); // fail for duplicate table name, make sure it re-bound the potentially cached statement
   fail();
  } catch (SQLException ex) {
   assertEquals(ex.getMessage(), "ERROR:  binding failed");
  }
  stmt.execute("DROP TABLE FOO;"); // will succeed, make sure it re-bound the potentially cached statement
  stmt.execute("DROP TABLE IF EXISTS FOO;");  // will succeed due to IF EXISTS, make sure it re-bound the potentially cached statement
  try {
   stmt.execute("DROP TABLE FOO;");  // fail for table not existing anymore, make sure it re-bound the potentially cached statement
   fail();
  } catch (SQLException ex) {
   assertEquals(ex.getMessage(), "ERROR:  binding failed");
  }
 } catch (SQLException ex) {
   DumpSQLException(ex);
 }
 }
}