package williampuk.timestamp.dbutils;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface DbConnSupplier {

    Connection get() throws SQLException;
}
