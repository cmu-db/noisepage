import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * MogJDBC is a simple CLI program to run arbitrary SQL test files over JDBC.
 * Currently,
 * - test file support: SQLite (partial)
 * - test database support: PostgreSQL (partial), SQLite (partial)
 * Extending support shouldn't be difficult, see MogSQLite and MogDb.
 */
@Command(description = "Run arbitrary SQL test files over JDBC.",
        name = "mogjdbc", mixinStandardHelpOptions = true, version = "mogjdbc 0.1")
public class MogJDBC implements Callable<Integer> {

    /**
     * Parameters for the JDBC connection to the database to be tested.
     */
    static class DbTestParams {
        @Option(required = true, names = {"--testJdbc"}, description = "dbTest JDBC connection string.")
        private String jdbc;
        @Option(required = true, names = {"--testUser"}, description = "dbTest username.")
        private String user;
        @Option(required = true, names = {"--testPass"}, description = "dbTest password.")
        private String pass;
    }

    /**
     * Parameters for the JDBC connection to the reference database.
     */
    static class DbRefParams {
        @Option(required = true, names = {"--refJdbc"}, description = "dbRef JDBC connection string.")
        private String jdbc;
        @Option(required = true, names = {"--refUser"}, description = "dbRef username.")
        private String user;
        @Option(required = true, names = {"--refPass"}, description = "dbRef password.")
        private String pass;
    }

    @ArgGroup(exclusive = false, multiplicity = "1")
    private DbTestParams dbTestParams;
    @ArgGroup(exclusive = false, multiplicity = "0..1")
    private DbRefParams dbRefParams;
    @Option(names = {"-fs", "--files-sqlite"}, description = "filesSqlite folder.")
    private File filesSqlite;
    @Option(names = {"-fm", "--files-mysql"}, description = "filesMysql folder.")
    private File filesMysql;
    @Option(names = {"-fp", "--files-postgres"}, description = "filesPostgres folder.")
    private File filesPostgres;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new MogJDBC()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        MogDb mogDb;

        /* Set test database. */
        mogDb = new MogDb(dbTestParams.jdbc, dbTestParams.user, dbTestParams.pass);
        System.out.printf("Test Database: [%s - %s - %s]\n",
                dbTestParams.jdbc, dbTestParams.user, dbTestParams.pass);

        /* Set reference database, if it exists. */
        if (null != dbRefParams) {
            mogDb.setDbRef(dbRefParams.jdbc, dbRefParams.user, dbRefParams.pass);
            System.out.printf("Reference Database: [%s - %s - %s]\n",
                    dbRefParams.jdbc, dbRefParams.user, dbRefParams.pass);
        }

        /* Run SQLite tests, if applicable. */
        if (null != filesSqlite) {
            System.out.printf("Running SQLite tests in: %s\n", filesSqlite.toPath());

            /* For each file in the SQLite test directory, TODO(WAN): recursive support? */
            for (File file : Objects.requireNonNull(filesSqlite.listFiles())) {
                /* If the file name ends with .test, MogJDBC assumes that it is a SQLite test file. */
                if (file.getName().endsWith(".test")) {
                    /* And will run the tests accordingly. */
                    try {
                        /* Refresh the database connections. */
                        mogDb.getDbTest().newConn();
                        mogDb.getDbRef().newConn();

                        int numRecords = 0;
                        int numPassed = 0;

                        /* Go through each record in the .test file. */
                        MogSqlite mogSqlite = new MogSqlite(file);
                        while (mogSqlite.next()) {
                            numPassed = mogSqlite.check(mogDb) ? numPassed + 1 : numPassed;
                            ++numRecords;
                        }

                        /* Report test status. */
                        System.out.printf("\tTest: %s %s [%d/%d passed]\n",
                                file.getName(), numRecords == numPassed ? "[OK]" : "[FAIL]", numPassed, numRecords);
                    } catch (Exception e) {
                        System.out.printf("\tError running: %s\n", file.getName());
                        e.printStackTrace();
                    }
                }
            }
        }

        return 0;
    }
}