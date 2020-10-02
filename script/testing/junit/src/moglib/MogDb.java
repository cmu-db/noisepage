package moglib;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static moglib.MogDb.DbColumnType.*;

/**
 * MogDb is an abstraction around the notion of a "testing database" and "reference database".
 * The "testing database" is the database that is to be tested.
 * The "reference database" is the database that acts as an authoritative reference implementation. It may be null.
 */
public class MogDb {

    /**
     * DbColumnType is an abstraction around the actual column types of various databases.
     * TODO(WAN): This was coded to SQLite's test format. It may be too SQLite-specific.
     */
    public enum DbColumnType {INVALID, INTEGER, TEXT, FLOAT}

    /**
     * Database (for lack of better naming) abstracts around connecting to and querying different JDBC databases.
     */
    public class Database {
        /**
         * Database JDBC url.
         */
        private String jdbc;
        /**
         * Database username.
         */
        private String user;
        /**
         * Database password.
         */
        private String pass;
        /**
         * Connection to the database.
         */
        private Connection conn = null;

        /**
         * Create a new Database abstraction. Note that no connection is made until newConn() is called.
         *
         * @param jdbc The JDBC URL.
         * @param user The username.
         * @param pass The password. Yep, plain-text, stored right there in memory.
         */
        public Database(String jdbc, String user, String pass) {
            this.jdbc = jdbc;
            this.user = user;
            this.pass = pass;
        }

        /**
         * Close any existing connection and create a new connection to the database.
         *
         * @return A new connection to the specified database.
         * @throws SQLException If a new connection could not be made.
         */
        public Connection newConn()  {
            if (null != this.conn) {
                try {
                    this.conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            try {
                this.conn = DriverManager.getConnection(this.jdbc, this.user, this.pass);
            } catch (SQLException throwables) {
                System.out.println("enter here");
                throwables.printStackTrace();
            }
            return this.conn;
        }

        /**
         * Get the current database connection. newConn() should have been called!
         *
         * @return The current database connection, which may be null if newConn() was not called.
         */
        public Connection getConn() {
            return this.conn;
        }

        /**
         * Get MogDb's internal representation of the given JDBC column type name.
         *
         * @param typeName Column type name provided by the JDBC connection to this database.
         * @return MogDb column type representation.
         */
        public DbColumnType getDbColumnType(String typeName) {
            // TODO(WAN): this requires more information.
            /*
             * TODO(WAN): This reequires more information on what each JDBC column type name gets reported
             *  back as. In practice, just run through the trace, look at the warning messages that complain
             *  about type mismatches, and add the appropriate types here.
             */

            if (this.jdbc.startsWith("jdbc:postgresql")) {
                /* Postgres types. */
                if (typeName.equals("int2") || typeName.equals("int4") || typeName.equals("int8")) {
                    return INTEGER;
                } else if (typeName.equals("varchar") || typeName.equals("text")) {
                    return DbColumnType.TEXT;
                } else if(typeName.equals("float8") || typeName.equals("numeric")){
                    return DbColumnType.FLOAT;
                }
            } else if (this.jdbc.startsWith("jdbc:sqlite")) {
                /* SQLite types. */
                if (typeName.equals("INTEGER") || typeName.equals("NUMERIC")) {
                    return INTEGER;
                } else if (typeName.equals("VARCHAR")) {
                    return TEXT;
                }
            }
            return INVALID;
        }
    }

    /**
     * Database to be tested.
     */
    private Database dbTest;
    /**
     * Reference database.
     */
    private Database dbRef = null;

    /**
     * Create a new MogDb instance with the specified test database.
     *
     * @param dbTestJdbc JDBC connection string for the test database.
     * @param dbTestUser Username for the test database.
     * @param dbTestPass Password for the test database.
     */
    public MogDb(String dbTestJdbc, String dbTestUser, String dbTestPass) {
        this.dbTest = new Database(dbTestJdbc, dbTestUser, dbTestPass);
    }

    /**
     * Set the reference database to be used.
     *
     * @param dbRefJdbc JDBC connection string for the reference database.
     * @param dbRefUser Username for the reference database.
     * @param dbRefPass Password for the reference database
     */
    public void setDbRef(String dbRefJdbc, String dbRefUser, String dbRefPass) {
        this.dbRef = new Database(dbRefJdbc, dbRefUser, dbRefPass);
    }

    /**
     * Get the database to be tested.
     *
     * @return The database to be tested.
     */
    public Database getDbTest() {
        return this.dbTest;
    }

    /**
     * Get the reference database.
     *
     * @return The reference database.
     */
    public Database getDbRef() {
        return this.dbRef;
    }
}
