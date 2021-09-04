package williampuk.timestamp.dbutils;

import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableSupplier;

import java.sql.Connection;
import java.sql.SQLException;

public class DbUtils {
    public static void withDbConn(final FailableSupplier<Connection, SQLException> connSupplier, final FailableConsumer<Connection, SQLException> connConsumer) {
        try (final Connection conn = connSupplier.get()) {
            connConsumer.accept(conn);
        } catch (SQLException sqle) {
            throw new RuntimeException(sqle);
        }
    }
}
