package moglib;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

// TODO(WAN): document, clean-up.

public class MogSqlite {

    private enum SqliteMode {INVALID, RECORD_STATEMENT_OK, RECORD_STATEMENT_ERROR, RECORD_QUERY}

    /**
     * The maximum read-ahead for cases where "----" does not appear to mark query record termination.
     */
    private static final int RECORD_READAHEAD_LIMIT = 1024;

    private SqliteMode mode = SqliteMode.INVALID;
    private StringBuilder sb = new StringBuilder();
    public BufferedReader br;
    public String sql = "";

    /* Query records. */
    public String typeString = "";
    public String sortMode = "";
    public String label = "";
    public ArrayList<String> queryResults = new ArrayList<>();
    public String queryFirstLine;
    public ArrayList<String> comments = new ArrayList<>();
    public int lineCounter = 0;
    public int lineNum;

    public MogSqlite(File sqliteTestFile) throws FileNotFoundException {
        this.br = new BufferedReader(new FileReader(sqliteTestFile));
    }

    /**
     * Set the state to be the next record.
     *
     * @return True if there is a next record and false otherwise.
     * @throws IOException      If there was an error in reading the file.
     * @throws RuntimeException
     */
    public boolean next() throws IOException, RuntimeException {
        boolean readLine = false;
        String line;
        while (null != (line = br.readLine())) {
            readLine = true;
            if (line.startsWith(Constants.HASHTAG)||line.startsWith(Constants.SKIPIF)
                    ||(line.startsWith(Constants.ONLYIF))) {
                /* Ignore comments. */
                lineCounter++;
                comments.add(line);
                continue;
            } else if (line.startsWith(Constants.HALT)) {
                /* Special debugging control record, ignore the rest of the test script. */
                lineCounter++;
                return false;
            } else if (line.startsWith(Constants.HASH_THRESHOLD)) {
                /* Ignore hash-threshold control record. */
                lineCounter++;
                continue;
            } else if (line.startsWith(Constants.STATEMENT_OK) || line.startsWith(Constants.STATEMENT_ERROR)) {
                /* Statement record. */
                queryFirstLine = line.trim();
                lineCounter++;
                readRecordStatement(line);
                break;
            }  else if (line.startsWith(Constants.QUERY)) {
                /* Query record. */
                queryFirstLine = line.trim();
                lineNum = lineCounter;
                readRecordQuery(line);
                break;
            } else if (!line.equals("")) {
                throw new RuntimeException("Invalid record type encountered.");
            }
        }

        return readLine;
    }

    public boolean check(MogDb mogDb) throws SQLException {
        if (SqliteMode.RECORD_STATEMENT_OK == this.mode) {
            return checkRecordStatementOk(mogDb);
        } else if (SqliteMode.RECORD_QUERY == this.mode) {
            return checkRecordQuery(mogDb);
        } else {
            throw new RuntimeException("unimplemented");
        }
    }

    private boolean checkRecordStatement(Connection conn) throws SQLException {
        Statement statement = conn.createStatement();
        boolean hasResults = statement.execute(this.sql);
        if (hasResults) {
            System.err.println("Statement records should have no results.");
            return false;
        }
        return true;
    }

    private boolean checkRecordStatementOk(MogDb mogDb) throws SQLException {
        return checkRecordStatement(mogDb.getDbTest().getConn()) && checkRecordStatement(mogDb.getDbRef().getConn());
    }

    private boolean checkRecordQuery(MogDb mogDb) throws SQLException {
        Statement statement = mogDb.getDbTest().getConn().createStatement();
        statement.execute(this.sql);
        ResultSet rs = statement.getResultSet();
        ResultSet refRs = null;

        if (null != mogDb.getDbRef().getConn()) {
            Statement refStatement = mogDb.getDbRef().getConn().createStatement();
            refStatement.execute(this.sql);
            refRs = refStatement.getResultSet();
        }

        boolean ok = true;

        /* Check if the type string is OK. */
        ok = ok && checkTypeString(mogDb, rs, refRs);
        /* Check the actual results. */
        ok = ok && checkResults(rs, refRs);

        statement.close();

        return ok;
    }

    private boolean checkTypeString(MogDb mogDb, ResultSet rs, ResultSet refRs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        ResultSetMetaData refRsmd = null == refRs ? null : refRs.getMetaData();

        /* Check against the reference result set. */
        if (null != refRsmd) {
            if (refRsmd.getColumnCount() != rsmd.getColumnCount()) {
                return false;
            } else {
                for (int i = 1; i <= refRsmd.getColumnCount(); ++i) {
                    String testColTypeName = rsmd.getColumnTypeName(i);
                    String refColTypeName = refRsmd.getColumnTypeName(i);
                    MogDb.DbColumnType testColType = mogDb.getDbTest().getDbColumnType(testColTypeName);
                    MogDb.DbColumnType refColType = mogDb.getDbRef().getDbColumnType(refColTypeName);

                    if (testColType != refColType) {
                        System.err.printf("Mismatched types, test %s and ref %s.\n", testColTypeName, refColTypeName);
                        return false;
                    }
                }
                // TODO(WAN): option to skip the trace or not
                return true;
            }
        }

        if (rsmd.getColumnCount() != typeString.length()) {
            System.err.printf("Mismatch count, actual %d expected %d\n.", rsmd.getColumnCount(), typeString.length());
            return false;
        }
        for (int i = 1; i <= typeString.length(); ++i) {
            String colTypeName = rsmd.getColumnTypeName(i);
            MogDb.DbColumnType colType = mogDb.getDbTest().getDbColumnType(colTypeName);
            char colTypeChar = typeString.charAt(i - 1);

            if (false
                    || ('T' == colTypeChar && MogDb.DbColumnType.TEXT != colType)
                    || ('I' == colTypeChar && MogDb.DbColumnType.INTEGER != colType)
                    || ('R' == colTypeChar && MogDb.DbColumnType.FLOAT != colType)) {
                System.err.printf("Mismatched type string %s and column type name %s\n.", typeString, colTypeName);
                return false;
            }
        }
        return true;
    }

    public List<String> processResults(ResultSet rs) throws SQLException {
        int numCols = rs.getMetaData().getColumnCount();
        ArrayList<ArrayList<String>> resultRows = new ArrayList<>();
        while (rs.next()) {
            ArrayList<String> resultRow = new ArrayList<>();
            for (int i = 1; i <= numCols; ++i) {
                // TODO(WAN): expose NULL behavior as knob
                if (null == rs.getString(i)) {
                    resultRow.add("");
                } else {
                    resultRow.add(rs.getString(i));
                }
            }
            resultRows.add(resultRow);
        }

        /*
         * Perform any necessary sorting. Logic is a little tangled here.
         * 1. First, rowsort if necessary.
         * 2. Then flatten the List<List<String>> into a List<String>.
         * 3. Now, valuesort if necessary.
         * 4. nosort is the default.
         *
         * This logic handles the nosort, rowsort, and valuesort cases properly with minimal branching.
         * But adding new sorting cases may require a rewrite.
         */
        if (this.sortMode.equals("rowsort")) {
            /* Sort each row individually. */
            resultRows.sort(new Comparator<ArrayList<String>>() {
                @Override
                public int compare(ArrayList<String> o1, ArrayList<String> o2) {
                    int c = 0;
                    for (int i = 0; c == 0 && i < numCols; ++i) {
                        c = o1.get(i).compareTo(o2.get(i));
                    }
                    return c;
                }
            });
        }
        return resultRows.stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    private boolean checkResults(ResultSet rs, ResultSet refRs) throws SQLException {
        List<String> results = processResults(rs);
        List<String> refResults = null == refRs ? null : processResults(refRs);

        if (null != refResults) {
            if (refResults.size() != results.size()) {
                System.err.printf("Mismatched sizes, actual %d reference %d.\n", results.size(), refResults.size());
                return false;
            }
            for (int i = 0; i < refResults.size(); ++i) {
                if (!refResults.get(i).equals(results.get(i))) {
                    System.err.printf("Mismatched data, actual %s reference %s.\n", results.get(i), refResults.get(i));
                    return false;
                }
            }
            // TODO(WAN): expose option to skip trace or not
            return true;
        }

        /* Check if the number of results is as expected. */
        if (results.size() != queryResults.size()) {
            /* If not, it could be because of the hash-threshold of the .test format. */
            if (1 == queryResults.size()) {
                /* String should be of format "NUM_VALUES values hashing to MD5_HASH". */
                String[] checker = queryResults.get(0).split(" ");

                /* Check the number of values. */
                if (!checker[0].equals(String.valueOf(results.size()))) {
                    /* Different number of values, fail. */
                    System.err.printf("Result count mismatch: expected %s got %d\n", checker[0], results.size());
                    return false;
                }

                /* Check the MD5_HASH. */
                MessageDigest md;
                try {
                    md = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("Sucks.", e);
                }

                /* Hash the string. The required format is from sqllogictest.c. */
                String resultsString = String.join("\n", results) + "\n";
                md.update(resultsString.getBytes());
                String resultsHash = MogUtil.bytesToHex(md.digest());

                if (checker[4].equalsIgnoreCase(resultsHash)) {
                    /* Same MD5 hash, pass. */
                    return true;
                } else {
                    System.err.println(this.sql);
                    System.err.printf("Result hash mismatch: expected %s got %s\n", checker[4], resultsHash);
                    return false;
                }
            }
            System.err.printf("Mismatch count, actual %d expected %d.\n", results.size(), queryResults.size());
            return false;
        }

        /* If here, the result size matches. Compare results individually. */
        for (int i = 0; i < results.size(); ++i) {
            if (!results.get(i).equals(queryResults.get(i))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Read a statement record.
     *
     * @param line The first line of the statement record, of the form "statement ok" or "statement error".
     * @throws IOException If reading the file suddenly fails.
     */
    private void readRecordStatement(String line) throws IOException {
        if (line.startsWith(Constants.STATEMENT_OK)) {
            this.mode = SqliteMode.RECORD_STATEMENT_OK;
        } else if (line.startsWith(Constants.STATEMENT_ERROR)) {
            this.mode = SqliteMode.RECORD_STATEMENT_ERROR;
        } else {
            assert (false);
        }

        while (true) {
            this.br.mark(RECORD_READAHEAD_LIMIT);
            line = this.br.readLine();

            if (null == line || line.startsWith(Constants.QUERY) || line.startsWith(Constants.STATEMENT_OK)
                    || line.startsWith(Constants.STATEMENT_ERROR) ||line.startsWith(Constants.HASHTAG)
                    || line.startsWith(Constants.SKIPIF) || line.startsWith(Constants.ONLYIF)) {
                /* End of SQL query reached. */
                this.br.reset();
                this.sql = this.sb.toString();
                this.sb.setLength(0);
                break;
            } else {
                /* Part of SQL query, keep going. */
                this.sb.append(line.trim());
                this.sb.append("\n");
            }
        }
    }

    /**
     * Read a query record.
     *
     * @param line The first line of the query record, of the form "query <type-string> <sort-mode> <label>".
     * @throws IOException If reading the file suddenly fails.
     */
    private void readRecordQuery(String line) throws IOException {
        assert (line.startsWith(Constants.QUERY));
        this.mode = SqliteMode.RECORD_QUERY;

        /* Parse the query string arguments. */
        String[] args = line.split(" ");
        this.typeString = args[1];
        if (args.length >= 3) {
            if (args[2].equals("nosort") || args[2].equals("rowsort") || args[2].equals("valuesort")) {
                /* If the sortMode is specified, then set it. Last argument (if any) must be the label. */
                this.sortMode = args[2];
                if (args.length >= 4) {
                    this.label = args[3];
                }
            } else {
                /* Must be the label. */
                this.label = args[2];
            }
        }

        /* Read the SQL query itself. */
        while (true) {
            this.br.mark(RECORD_READAHEAD_LIMIT);
            line = this.br.readLine();

            if (isNextRecord(line) || line.startsWith(Constants.SEPARATION)) {
                /* End of SQL query reached. If it was not the query record terminator, go back one line. */
                if (null == line || !line.startsWith(Constants.SEPARATION)) {
                    this.br.reset();
                }
                this.sql = this.sb.toString();
                this.sb.setLength(0);
                break;
            } else {
                /* Part of SQL query, keep going. */
                this.sb.append(line.trim());
                this.sb.append("\n");
            }
        }
        /* At this point, the buffered reader will have finished reading until the "----" inclusive, if it exists. */

        /* Read the SQL query results. */
        queryResults.clear();
        while (true) {
            this.br.mark(RECORD_READAHEAD_LIMIT);
            line = this.br.readLine();

            if (isNextRecord(line)) {
                /* End of SQL results. */
                this.br.reset();
                break;
            } else {
                if (!line.equals("")) {
                    queryResults.add(line);
                }
            }
        }
    }

    private boolean isNextRecord(String line) {
        return null == line
                || line.startsWith(Constants.QUERY)
                || line.startsWith(Constants.STATEMENT_OK)
                || line.startsWith(Constants.STATEMENT_ERROR)
                || line.startsWith(Constants.HASHTAG)
                || line.startsWith(Constants.SKIPIF)
                || line.startsWith(Constants.ONLYIF);
    }

}
