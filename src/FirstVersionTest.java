import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by kevin on 15-12-4.
 */
public class FirstVersionTest {

    public static String fname_entity = "/media/kevin/D/database/test_entity";
    public static String fname_property = "/media/kevin/D/database/test_property";
    public static String fname_type = "/media/kevin/D/database/test_type";

    public static void test1() {
        FBFileReader reader = new FBFileReader(100, new onProcessListener() {
            PreparedStatement stmt;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt = conn.prepareStatement("SELECT Entity_ID id, name FROM Entity WHERE name = ? LIMIT 0, 50");
                t2 = 0;
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt.setString(1, strs[2]);

                t1 = System.currentTimeMillis();
                stmt.executeQuery();
                t1 = System.currentTimeMillis() - t1;
//                System.out.println(t1);
                t2 += t1;
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                System.out.println(t2);
            }
        });
        reader.read(fname_entity, Main.url);
    }

    public static void test2() {
        FBFileReader reader = new FBFileReader(100, new onProcessListener() {
            PreparedStatement stmt;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt = conn.prepareStatement("SELECT Type_URI id FROM EntityType WHERE Entity_ID = ? LIMIT 0, 50");

            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt.setString(1, strs[1]);

                t1 = System.currentTimeMillis();
                stmt.executeQuery();
                t1 = System.currentTimeMillis() - t1;
                t2 += t1;
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                System.out.println(t2);
            }
        });
        reader.read(fname_entity, Main.url);
    }

    public static void test3() {
        FBFileReader reader = new FBFileReader(100, new onProcessListener() {
            PreparedStatement stmt;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt = conn.prepareStatement("SELECT Property.Property_URI FROM EntityType, Property WHERE EntityType.Entity_ID = ? AND (EntityType.Type_URI = Property.domain OR EntityType.Type_URI = Property.range) LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt.setString(1, strs[1]);

                t1 = System.currentTimeMillis();
                stmt.executeQuery();
                t1 = System.currentTimeMillis() - t1;
                t2 += t1;
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                System.out.println(t2);
            }
        });
        reader.read(fname_entity, Main.url);
    }

    public static void test4() {
        FBFileReader reader = new FBFileReader(100, new onProcessListener() {
            PreparedStatement stmt;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt = conn.prepareStatement("(SELECT sURI s, oURI o FROM RelationStatement WHERE sURI = ? OR oURI = ?) UNION " +
                        "(SELECT sURI s, oValue o FROM valuestatement WHERE sURI = ?) LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt.setString(1, strs[1]);
                stmt.setString(2, strs[1]);
                stmt.setString(3, strs[1]);

                t1 = System.currentTimeMillis();
                stmt.executeQuery();
                t1 = System.currentTimeMillis() - t1;
                t2 += t1;
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                System.out.println(t2);
            }
        });
        reader.read(fname_entity, Main.url);
    }

    public static void test5() {
        FBFileReader reader = new FBFileReader(100, new onProcessListener() {
            PreparedStatement stmt;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt = conn.prepareStatement("SELECT * FROM EntityType WHERE Type_URI = ? LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt.setString(1, strs[1]);

                t1 = System.currentTimeMillis();
                stmt.executeQuery();
                t1 = System.currentTimeMillis() - t1;
                t2 += t1;
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                System.out.println(t2);
            }
        });
        reader.read(fname_type, Main.url);
    }

    public static void test6() {
        FBFileReader reader = new FBFileReader(100, new onProcessListener() {
            PreparedStatement stmt;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt = conn.prepareStatement("SELECT * FROM Type WHERE Type_URI LIKE ? LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt.setString(1, strs[1].substring(0, strs[1].lastIndexOf(".")) + "%");

                t1 = System.currentTimeMillis();
                stmt.executeQuery();
                t1 = System.currentTimeMillis() - t1;
                t2 += t1;
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                System.out.println(t2);
            }
        });
        reader.read(fname_type, Main.url);
    }

    public static void test7() {
        FBFileReader reader = new FBFileReader(100, new onProcessListener() {
            PreparedStatement stmt;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt = conn.prepareStatement("SELECT sURI s, oURI o FROM RelationStatement WHERE pURI = ? LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt.setString(1, strs[1]);

                t1 = System.currentTimeMillis();
                stmt.executeQuery();
                t1 = System.currentTimeMillis() - t1;
                t2 += t1;
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                System.out.println(t2);
            }
        });
        reader.read(fname_property, Main.url);
    }
}
