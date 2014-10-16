package br.edu.utfpr.commitsloc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
    private static final String INSERT
            = "INSERT INTO commits_files_lines"
            + " (file_id, commit_id, added, removed) VALUES ("
            + "  (SELECT fil.id "
            + "     FROM files fil "
            + "     JOIN actions a ON fil.id = a.file_id "
            + "     JOIN file_links fill ON fill.file_id = fil.id "
            + "    WHERE fill.file_path LIKE ? "
            + "      AND fil.file_name = ? "
            + "      AND a.commit_id = (SELECT s.id FROM scmlog s WHERE s.rev = ?)"
            + "      AND fil.id NOT IN (SELECT cfl.file_id FROM commits_files_lines cfl WHERE cfl.commit_id = a.commit_id)"
            + "  ),(SELECT s.id FROM scmlog s WHERE s.rev = ?),?,?)";

    private static final String INSERT_FILTERING_BY_PARENT
            = "INSERT INTO commits_files_lines"
            + " (file_id, commit_id, added, removed) VALUES ("
            + "  (SELECT fil.id"
            + "     FROM files fil"
            + "     JOIN actions a ON fil.id = a.file_id"
            + "     JOIN file_links fill ON fill.file_id = fil.id"
            + "     JOIN file_links fillp ON fillp.file_id = fill.parent_id"
            + "     JOIN files filp ON filp.id = fillp.file_id"
            + "    WHERE fill.file_path LIKE ?"
            + "      AND fil.file_name = ? "
            + "      AND fillp.file_path LIKE ?"
            + "      AND filp.file_name = ?"
            + "      AND a.commit_id = (SELECT s.id FROM scmlog s WHERE s.rev = ?)"
            + "      AND fil.id NOT IN (SELECT cfl.file_id FROM commits_files_lines cfl WHERE cfl.commit_id = a.commit_id)"
            + "   ),(SELECT s.id FROM scmlog s WHERE s.rev = ?),?,?)";

    public static final String DIFF_HEADER = "INSERTED,DELETED,MODIFIED,FILENAME";
    public static final String[] REV1_REV2 = new String[]{"{1}", "{2}"};

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
                    + "id INTEGER PRIMARY KEY AUTO_INCREMENT,"
                    + "file_id INT(11),"
                    + "commit_id INT(11),"
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
                    "SELECT s.rev"
                    + "  FROM scmlog s"
                    + " WHERE s.date >"
                    + "       (SELECT COALESCE(MAX(s2.date), 0)"
                    + "          FROM commits_files_lines cfl "
                    + "          JOIN scmlog s2 ON s2.id = cfl.commit_id) "
                    + "   AND 50 >= "
                    + "       (SELECT count(1)"
                    + "          FROM aries_vcs.files fil"
                    + "          JOIN aries_vcs.actions a ON a.file_id = fil.id"
                    + "         WHERE s.id = a.commit_id) "
                    + " ORDER BY s.date ASC");

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
        String command = StringUtils.replaceEach(svnDiff, REV1_REV2,
                new String[]{String.valueOf(rev - 1), String.valueOf(rev)});

        Process p = Runtime.getRuntime().exec(new String[]{"/bin/bash", "-c", command});
        log.info(command);
        p.waitFor();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));

        List<String[]> lines = new ArrayList<>();

        // reads the output from command
        String readLine = reader.readLine(); // first line is = DIFF_HEADER
        while ((readLine = reader.readLine()) != null) {
            log.info(readLine);
            lines.add(StringUtils.split(readLine, COLUMN_SEPARATOR));
        }

        log.info("Files: " + lines.size());
        // order by file length
        // workaround for duplicated filename + path issue
        Collections.sort(lines, new Comparator<String[]>() {

            @Override
            public int compare(String[] o1, String[] o2) {
                return o1[3].length() > o2[3].length() ? -1 : 1;
            }
        });

        for (String[] line : lines) {
            insert(line, rev);
        }

        // read any errors from the attempted command
        while ((readLine = error.readLine()) != null) {
            log.info(readLine);
        }
    }

    private static void insert(String[] outputLine, int rev) throws SQLException {
        
        int inserted = Integer.valueOf(outputLine[0]);
        int deleted = Integer.valueOf(outputLine[1]);
        String filepath = outputLine[3];
        Path p = Paths.get(filepath);
        String filename = p.getFileName().toString();

        PreparedStatement stmt;
        if (p.getNameCount() > 1) { // path has parent name
            stmt = conn.prepareStatement(INSERT_FILTERING_BY_PARENT);
            stmt.setString(1, "%" + filepath);
            stmt.setString(2, filename);
            stmt.setString(3, p.getParent().getNameCount() > 1 ? "%" + p.getParent().toString() : p.getParent().toString());
            stmt.setString(4, p.getParent().getFileName().toString());
            stmt.setInt(5, rev);
            stmt.setInt(6, rev);
            stmt.setInt(7, inserted);
            stmt.setInt(8, deleted);
        } else {
            stmt = conn.prepareStatement(INSERT);
            stmt.setString(1, filename);
            stmt.setString(2, filename);
            stmt.setInt(3, rev);
            stmt.setInt(4, rev);
            stmt.setInt(5, inserted);
            stmt.setInt(6, deleted);
        }
        try {
            stmt.executeUpdate();
        } catch (java.sql.SQLException e) {
            log.error("filepath: " + filepath);
            log.error("filename: " + filename);
            log.error("parentpath: " + p.getParent().toString());
            log.error("parentname: " + p.getParent().getFileName().toString());
            log.error("rev: " + rev);
            throw e;
        }
    }

}
