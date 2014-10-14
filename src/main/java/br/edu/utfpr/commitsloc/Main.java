package br.edu.utfpr.commitsloc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Rodrigo T. Kuroda
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static final char COLUMN_SEPARATOR = ',';

//     -x [--extensions] ARG : Specify differencing options for external diff or
//                             internal diff or blame. Default: '-u'. Options are
//                             separated by spaces. Internal diff and blame take:
//                               -u, --unified: Show 3 lines of unified context
//                               -b, --ignore-space-change: Ignore changes in
//                                 amount of white space
//                               -w, --ignore-all-space: Ignore all white space
//                               --ignore-eol-style: Ignore changes in EOL style
//                               -p, --show-c-function: Show C function name
    private static final String SVN_DIFF = "svn diff -x -bw -r {1}:{2} {3} | diffstat -t";
    private static final String INSERT = "INSERT INTO commits_files_lines"
            + " ( file_links_id,"
            + " added,"
            + " removed)"
            + " VALUES"
            + " ((SELECT id FROM file_links WHERE file_path LIKE '%?' AND commit_id = (SELECT id FROM scmlog WHERE rev = ?)),?,?)";

    private static HikariDataSource ds;
    private static Connection conn;

    private static HikariDataSource getDatasource(String databaseName) {
        HikariConfig config = new HikariConfig();
        config.setMaximumPoolSize(100);
        config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        config.addDataSourceProperty("serverName", "localhost");
        config.addDataSourceProperty("port", "3306");
        config.addDataSourceProperty("databaseName", databaseName);
        config.addDataSourceProperty("user", "root");
        config.addDataSourceProperty("password", "root");
        config.setConnectionTestQuery("SELECT 1");

        return new HikariDataSource(config);
    }

    public static void main(String[] args) throws SQLException {
        try {
            if (args.length == 0 || StringUtils.isBlank(args[0])) {
                log.info("Inform the database name.");
                System.exit(1);
            }

            String databaseName = args[0];
            log.info("Database name: " + databaseName);

            ds = getDatasource(databaseName);
            conn = ds.getConnection();

            Statement createTable = conn.createStatement();
            createTable.execute("CREATE TABLE IF NOT EXISTS commits_files_lines ("
                    + "id INTEGER PRIMARY KEY,"
                    + "file_links_id INT(11),"
                    + "added INTEGER,"
                    + "removed INTEGER)");

            Statement uriStmt = conn.createStatement();
            ResultSet uri = uriStmt.executeQuery(
                    "SELECT uri "
                    + "FROM repositories WHERE id = 1");
            uri.next();

            String svnDiff = StringUtils.replace(SVN_DIFF, "{3}", uri.getString(1));

            Statement revisionsStmt = conn.createStatement();
            ResultSet revisions = revisionsStmt.executeQuery(
                    "SELECT rev "
                    + "FROM scmlog WHERE id > (SELECT COALESCE(MAX(commit_id), 0) "
                    + "FROM commits_files_lines cfl "
                    + "JOIN file_links fl ON fl.id = cfl.file_links_id) "
                    + "ORDER BY rev");

            while (revisions.next()) {
                int rev = revisions.getInt(1);
                executeSvnDiff(svnDiff, rev);
            }

        } catch (Exception ex) {
            log.error("Error: ", ex);
            System.exit(1);
        } finally {
            log.info("Closing datasource...");
            try {
                ds.shutdown();
            } catch (Exception e) {
                log.info("Datasource already closed.");
            }
        }
        System.exit(0);
    }

    private static void executeSvnDiff(String svnDiff, int rev) throws IOException, InterruptedException, SQLException {
        String command = StringUtils.replaceEach(svnDiff,
                new String[]{"{1}", "{2}"},
                new String[]{String.valueOf(rev - 1), String.valueOf(rev)});

        Process p = Runtime.getRuntime().exec(command);
        p.waitFor();
        BufferedReader reader
                = new BufferedReader(new InputStreamReader(p.getInputStream()));

        log.info(command);
        String line;
        while ((line = reader.readLine()) != null) {
            if (StringUtils.isNotBlank(line)) {
                insert(line, rev);
            }
        }
    }

    private static void insert(String outputLine, int rev) throws SQLException {
        // INSERTED, DELETED, MODIFIED, FILENAME
        String[] split = StringUtils.split(outputLine, COLUMN_SEPARATOR);
        PreparedStatement stmt = conn.prepareStatement(INSERT);
        int inserted = Integer.valueOf(split[0]);
        int deleted = Integer.valueOf(split[1]);
        String filename = split[3];

        stmt.setString(1, filename);
        stmt.setInt(2, rev);
        stmt.setInt(1, inserted);
        stmt.setInt(2, deleted);
        stmt.executeUpdate();
    }

}
