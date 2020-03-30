/**
 * Basic bootstrap test
 */

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TPCCBootstrapTest extends TestUtility {
    private Connection conn;

    private static final String SQL_CREATE_ITEM =
        "CREATE TABLE item (" +
        "i_id int NOT NULL PRIMARY KEY," +
        "i_name varchar(24) NOT NULL," +
        "i_price decimal NOT NULL," +
        "i_data varchar(50) NOT NULL," +
        "i_im_id int NOT NULL" +
        ");";

    private static final String SQL_CREATE_WAREHOUSE =
        "CREATE TABLE warehouse (" +
        "w_id int NOT NULL PRIMARY KEY," +
        "w_ytd decimal NOT NULL," +
        "w_tax decimal NOT NULL," +
        "w_name varchar(10) NOT NULL," +
        "w_street_1 varchar(20) NOT NULL," +
        "w_street_2 varchar(20) NOT NULL," +
        "w_city varchar(20) NOT NULL," +
        "w_state char(2) NOT NULL," +
        "w_zip char(9) NOT NULL" +
        ");";

    private static final String SQL_CREATE_STOCK =
        "CREATE TABLE stock (" +
        "s_w_id int NOT NULL PRIMARY KEY," +
        "s_i_id int NOT NULL PRIMARY KEY," +
        "s_quantity int NOT NULL," +
        "s_ytd decimal NOT NULL," +
        "s_order_cnt int NOT NULL," +
        "s_remote_cnt int NOT NULL," +
        "s_data varchar(50) NOT NULL," +
        "s_dist_01 char(24) NOT NULL," +
        "s_dist_02 char(24) NOT NULL," +
        "s_dist_03 char(24) NOT NULL," +
        "s_dist_04 char(24) NOT NULL," +
        "s_dist_05 char(24) NOT NULL," +
        "s_dist_06 char(24) NOT NULL," +
        "s_dist_07 char(24) NOT NULL," +
        "s_dist_08 char(24) NOT NULL," +
        "s_dist_09 char(24) NOT NULL," +
        "s_dist_10 char(24) NOT NULL" +
        ");";

    private static final String SQL_CREATE_DISTRICT =
        "CREATE TABLE district (" +
        "d_w_id int NOT NULL PRIMARY KEY," +
        "d_id int NOT NULL PRIMARY KEY," +
        "d_ytd decimal NOT NULL," +
        "d_tax decimal NOT NULL," +
        "d_next_o_id int NOT NULL," +
        "d_name varchar(10) NOT NULL," +
        "d_street_1 varchar(20) NOT NULL," +
        "d_street_2 varchar(20) NOT NULL," +
        "d_city varchar(20) NOT NULL," +
        "d_state char(2) NOT NULL," +
        "d_zip char(9) NOT NULL" +
        ");";

    private static final String SQL_CREATE_CUSTOMER =
        "CREATE TABLE customer (" +
        "c_w_id int NOT NULL PRIMARY KEY," +
        "c_d_id int NOT NULL PRIMARY KEY," +
        "c_id int NOT NULL PRIMARY KEY," +
        "c_discount decimal NOT NULL," +
        "c_credit char(2) NOT NULL," +
        "c_last varchar(16) NOT NULL," +
        "c_first varchar(16) NOT NULL," +
        "c_credit_lim decimal NOT NULL," +
        "c_balance decimal NOT NULL," +
        "c_ytd_payment float NOT NULL," +
        "c_payment_cnt int NOT NULL," +
        "c_delivery_cnt int NOT NULL," +
        "c_street_1 varchar(20) NOT NULL," +
        "c_street_2 varchar(20) NOT NULL," +
        "c_city varchar(20) NOT NULL," +
        "c_state char(2) NOT NULL," +
        "c_zip char(9) NOT NULL," +
        "c_phone char(16) NOT NULL," +
        "c_since timestamp NOT NULL," +
        "c_middle char(2) NOT NULL," +
        "c_data varchar(500) NOT NULL" +
        ");";

    private static final String SQL_CREATE_HISTORY =
        "CREATE TABLE history (" +
        "h_c_id int NOT NULL," +
        "h_c_d_id int NOT NULL," +
        "h_c_w_id int NOT NULL," +
        "h_d_id int NOT NULL," +
        "h_w_id int NOT NULL," +
        "h_date timestamp NOT NULL," +
        "h_amount decimal NOT NULL," +
        "h_data varchar(24) NOT NULL" +
        ");";

    private static final String SQL_CREATE_NEW_ORDER =
        "CREATE TABLE new_order (" +
        "no_w_id int NOT NULL PRIMARY KEY," +
        "no_d_id int NOT NULL PRIMARY KEY," +
        "no_o_id int NOT NULL PRIMARY KEY" +
        ");";

    private static final String SQL_CREATE_ORDER =
        "CREATE TABLE oorder (" +
        "o_w_id int NOT NULL PRIMARY KEY," +
        "o_d_id int NOT NULL PRIMARY KEY," +
        "o_id int NOT NULL PRIMARY KEY," +
        "o_c_id int NOT NULL," +
        "o_carrier_id int," +
        "o_ol_cnt decimal NOT NULL," +
        "o_all_local decimal NOT NULL," +
        "o_entry_d timestamp NOT NULL" +
        ");";

    private static final String SQL_CREATE_ORDER_LINE =
        "CREATE TABLE order_line (" +
        "ol_w_id int NOT NULL PRIMARY KEY," +
        "ol_d_id int NOT NULL PRIMARY KEY," +
        "ol_o_id int NOT NULL PRIMARY KEY," +
        "ol_number int NOT NULL PRIMARY KEY," +
        "ol_i_id int NOT NULL," +
        "ol_delivery_d timestamp," +
        "ol_amount decimal NOT NULL," +
        "ol_supply_w_id int NOT NULL," +
        "ol_quantity decimal NOT NULL," +
        "ol_dist_info char(24) NOT NULL" +
        ");";


    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS item;");
        stmt.execute("DROP TABLE IF EXISTS warehouse;");
        stmt.execute("DROP TABLE IF EXISTS stock;");
        stmt.execute("DROP TABLE IF EXISTS district;");
        stmt.execute("DROP TABLE IF EXISTS customer;");
        stmt.execute("DROP TABLE IF EXISTS history;");
        stmt.execute("DROP TABLE IF EXISTS new_order;");
        stmt.execute("DROP TABLE IF EXISTS oorder;");
        stmt.execute("DROP TABLE IF EXISTS order_line;");
        stmt.execute(SQL_CREATE_ITEM);
        stmt.execute(SQL_CREATE_WAREHOUSE);
        stmt.execute(SQL_CREATE_STOCK);
        stmt.execute(SQL_CREATE_DISTRICT);
        stmt.execute(SQL_CREATE_CUSTOMER);
        stmt.execute(SQL_CREATE_HISTORY);
        stmt.execute(SQL_CREATE_NEW_ORDER);
        stmt.execute(SQL_CREATE_ORDER);
        stmt.execute(SQL_CREATE_ORDER_LINE);
    }

    @Before
    public void setup() {
        try {
            conn = makeDefaultConnection();
            conn.setAutoCommit(true);
            initDatabase();
        } catch (SQLException ex) {
            DumpSQLException(ex);
        }
    }

    @After
    public void teardown() throws SQLException {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS item;");
            stmt.execute("DROP TABLE IF EXISTS warehouse;");
            stmt.execute("DROP TABLE IF EXISTS stock;");
            stmt.execute("DROP TABLE IF EXISTS district;");
            stmt.execute("DROP TABLE IF EXISTS customer;");
            stmt.execute("DROP TABLE IF EXISTS history;");
            stmt.execute("DROP TABLE IF EXISTS new_order;");
            stmt.execute("DROP TABLE IF EXISTS oorder;");
            stmt.execute("DROP TABLE IF EXISTS order_line;");
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

    @Test
    public void testCreateTablesandIndexes() throws SQLException {
    }
}
