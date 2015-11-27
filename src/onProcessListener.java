import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by kevin on 15-11-26.
 */
public interface onProcessListener {
    void onPrepare(Connection conn) throws SQLException, IOException;

    void onWhile(Connection conn, int nline, String line) throws SQLException, IOException;

    void onFinish(Connection conn, int nline) throws SQLException, IOException;
}
