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
 public void Teardown() throws SQLException {}


 /**
  * EmptyStatementTest
  */
 @Test
 public void test_EmptyStatement() throws SQLException {
  Statement stmt = conn.createStatement();
  stmt.execute("");
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
}