import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Main {
    public static String fname = "/media/kevin/D/database/sub-freebase";
    public static String url = "jdbc:mysql://localhost:3306/freebase?useServerPrepStmts=false&rewriteBatchedStatements=true&characterEncoding=utf8";
    public static String test_url = "jdbc:mysql://localhost:3306/new_schema?useServerPrepStmts=false&rewriteBatchedStatements=true&characterEncoding=utf8";

    private static void test_innodb_file_per_table() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(test_url, "root", "Kevin2015");
            conn.setAutoCommit(true);

            PreparedStatement stmt = conn.prepareStatement("INSERT INTO new_table VALUES (?)");

            for (int i = 0; i < 1000000; ++i) {
                stmt.setInt(1, i);
                stmt.executeUpdate();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        FirstVersion.insert_entity_type();
    }


}