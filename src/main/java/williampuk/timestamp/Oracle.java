package williampuk.timestamp;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.function.Failable;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.sql.Types.TIMESTAMP;
import static williampuk.timestamp.dbutils.DbUtils.withDbConn;

public class Oracle {

    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Hong_Kong"));
        prepareTable();
        insertData();
        readData();
        testSetTimestamp();
        testDaylightSaving();
    }

    private static void testDaylightSaving() {
        final TimeZone oldTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
        withDbConn(Oracle::getConnection, conn -> {
            final String sql = "SELECT TIMESTAMP '2021-03-14 02:01:01.0', TIMESTAMP '2021-03-14 03:01:01.0'," +
                    "  TIMESTAMP '2021-11-07 01:01:01.0' FROM DUAL";
            try (final Statement stmt = conn.createStatement();
                 final ResultSet rs = stmt.executeQuery(sql)) {
                System.out.println("=== Test default timezone observing daylight saving ===");
                rs.next();
                System.out.println(rs.getString(1) + " is converted to Timestamp of time instant: " +
                        rs.getTimestamp(1).toInstant().atZone(ZoneId.systemDefault()));
                System.out.println(rs.getString(2) + " is converted to Timestamp of time instant: " +
                        rs.getTimestamp(2).toInstant().atZone(ZoneId.systemDefault()));
                System.out.println(rs.getString(3) + " is converted to Timestamp of time instant: " +
                        rs.getTimestamp(3).toInstant().atZone(ZoneId.systemDefault()));
            }
        });
        TimeZone.setDefault(oldTz);
    }

    private static void testSetTimestamp() {
        withDbConn(Oracle::getConnection, conn -> {
            final String sql = "SELECT ?, ?, ? FROM DUAL";
            try (final PreparedStatement ps = conn.prepareStatement(sql)) {
                Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
                final Timestamp ts = Timestamp.from(Instant.now());
                ps.setTimestamp(1, ts, cal);
                ps.setString(2, ts.toInstant().atZone(cal.getTimeZone().toZoneId()).toLocalDateTime().toString());
                ps.setString(3, ts.toInstant().atZone(cal.getTimeZone().toZoneId()).toString());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString(2));
                    System.out.println(rs.getString(3));
                }
            }
        });
    }

    private static void readData() {
        withDbConn(Oracle::getConnection, conn -> {
            try (final Statement stmt = conn.createStatement()) {
                // Set session timezone to UTC+5
                stmt.execute("ALTER SESSION SET TIME_ZONE = '+05:00'");
                final String sql = "SELECT created_timestamp, " +
                        "  CAST(created_timestamp AS VARCHAR2(30 CHAR)) created_timestamp_str, " +
                        "  timestamp_val, CAST(timestamp_val AS VARCHAR2(30 CHAR)) timestamp_val_str, " +
                        "  remarks, " +
                        "  CAST(LOCALTIMESTAMP AS VARCHAR2(30 CHAR)) retrieved " +
                        "FROM timestamp_test";
                try (final ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        final ResultSetMetaData rsMetaData = rs.getMetaData();
                        final IntFunction<String> colIdxToResultMapper = i -> {
                            try {
                                return StringUtils.rightPad(rsMetaData.getColumnName(i) + ":", 25, ' ') +
                                        (rsMetaData.getColumnType(i) == TIMESTAMP ?
                                                String.format("'%s'%n%s'%s'",
                                                        rs.getTimestamp(i),
                                                        StringUtils.leftPad("(using NY Cal): ", 25, ' '),
                                                        rs.getTimestamp(i, Calendar.getInstance(
                                                                TimeZone.getTimeZone("America/New_York")))) :
                                                String.format("'%s'", rs.getString(i)));
                            } catch (SQLException sqle) {
                                throw Failable.rethrow(sqle);
                            }
                        };
                        final String rowData = IntStream.rangeClosed(1, rsMetaData.getColumnCount())
                                .mapToObj(colIdxToResultMapper)
                                .collect(Collectors.joining(System.lineSeparator()));
                        System.out.printf("[Time: %s] Row #%d:%n%s%n", Instant.now(), rs.getRow(), rowData);
                    }
                }
            }
        });
    }

    private static void insertData() {
        withDbConn(Oracle::getConnection, conn -> {
            final String sql = "INSERT INTO timestamp_test " +
                    "  (created_timestamp, timestamp_val, remarks)" +
                    "  VALUES " +
                    "  (LOCALTIMESTAMP, ?, ?)";
            try (final Statement s = conn.createStatement();
                 final PreparedStatement ps = conn.prepareStatement(sql)) {
                // Set session timezone
                s.execute(String.format("ALTER SESSION SET TIME_ZONE = '%s'", TimeZone.getDefault().getID()));

                final Timestamp tsValue = Timestamp.valueOf("2021-03-14 02:01:01");
                ps.setTimestamp(1, tsValue);
                ps.setString(2, String.format("Inserted value '%s' using JDBC at: %s",
                        tsValue.toInstant().atZone(ZoneId.systemDefault()),
                        ZonedDateTime.now()));
                ps.executeUpdate();
            }
        });
    }

    private static void prepareTable() {
        withDbConn(Oracle::getConnection, conn -> {
            // Credit: https://stackoverflow.com/a/50049759
            final String plSql = "BEGIN " +
                    "  FOR i IN (SELECT 1 FROM user_tables WHERE table_name = 'TIMESTAMP_TEST') LOOP " +
                    "    EXECUTE IMMEDIATE 'DROP TABLE TIMESTAMP_TEST CASCADE CONSTRAINTS PURGE'; " +
                    "  END LOOP; " +
                    "END;";
            try (final CallableStatement c = conn.prepareCall(plSql)) {
                c.execute();
            }
            try (final Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE timestamp_test (" +
                        "created_timestamp TIMESTAMP NOT NULL," +
                        "timestamp_val TIMESTAMP NULL," +
                        "remarks VARCHAR2(200 CHAR) NULL)");
            }
        });
    }

    private static Connection getConnection() throws SQLException {
        final Properties props = new Properties();
        props.setProperty("user", "oracle");
        props.setProperty("password", "oracle");
        return DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/XEPDB1", props);
    }

}
