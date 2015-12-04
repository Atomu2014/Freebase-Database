import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Main {
    public static String fname = "/media/kevin/D/database/sub-freebase";
    //    public static String url = "jdbc:mysql://172.16.2.62:3306/freebase?useServerPrepStmts=false&rewriteBatchedStatements=true&characterEncoding=utf8";
    public static String url = "jdbc:mysql://localhost:3306/freebase?useServerPrepStmts=false&rewriteBatchedStatements=true&characterEncoding=utf8";
    public static String test_url = "jdbc:mysql://localhost:3306/test?useServerPrepStmts=false&rewriteBatchedStatements=true&characterEncoding=utf8";

    private static void test_innodb_file_per_table() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(test_url, "root", "Kevin2015");
            conn.setAutoCommit(true);

            PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table_1 VALUES (?)");
            PreparedStatement stmt_2 = conn.prepareStatement("INSERT INTO test_table_2 VALUES (?)");

            for (int i = 0; i < 10000; ++i) {
                stmt.setInt(1, i);
                stmt_2.setString(1, "" + i);
                stmt.executeUpdate();
                stmt_2.executeUpdate();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        System.out.println("type");
//        FirstVersion.insert_type();
//        System.out.println("property");
//        FirstVersion.insert_property();
//        System.out.println("entity");
//        FirstVersion.insert_entity();
//        System.out.println("entity type");
//        FirstVersion.insert_entity_type();
//        System.out.println("relation statement");
//        FirstVersion.insert_rstste();

//        System.out.println("property");
//        SecondVersion.insert_property();
//        System.out.println("type");
//        SecondVersion.insert_type();
//        System.out.println("entity type");
//        SecondVersion.insert_entity_type();
//        System.out.println("entity");
//        SecondVersion.insert_entity();
//        System.out.println("fff");
//        SecondVersion.insert_fff();
//        System.out.println("ffn");
//        SecondVersion.insert_ffn();

        System.out.println("query 1");
        FirstVersionTest.test1();
    }


}