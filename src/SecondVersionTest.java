import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by kevin on 15-12-4.
 */
public class SecondVersionTest {
    public static String fname_entity = "/media/kevin/D/database/test_entity";
    public static String fname_property = "/media/kevin/D/database/test_property";
    public static String fname_type = "/media/kevin/D/database/test_type";

    public static void test1() {
        FBFileReader reader = new FBFileReader(100, new onProcessListener() {
            PreparedStatement stmt;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt = conn.prepareStatement("SELECT Entity_URI id, name FROM Entity WHERE MATCH(name) AGAINST(?) LIMIT 0, 50");
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
            PreparedStatement stmt2, stmt1;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt1 = conn.prepareStatement("SELECT Entity_ID id FROM Entity WHERE Entity_URI = ? LIMIT 0, 50");
                stmt2 = conn.prepareStatement("SELECT map.URI id FROM EntityType et JOIN idMap map ON (et.Type_ID = map.ID) WHERE et.Entity_ID = ? LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt1.setString(1, strs[1]);
                ResultSet res = stmt1.executeQuery();
                int id;
                if (res.next()) {
                    id = res.getInt("id");
                    stmt2.setInt(1, id);
                    t1 = System.currentTimeMillis();
                    stmt2.executeQuery();
                    t1 = System.currentTimeMillis() - t1;
                    t2 += t1;
                }

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
            PreparedStatement stmt1, stmt2;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt1 = conn.prepareStatement("SELECT Type_URI id FROM EntityType WHERE Entity_ID = ? LIMIT 0, 50");
                stmt2 = conn.prepareStatement("SELECT map.URI id FROM EntityType et, Property p JOIN idMap map ON (p.Property_ID = map.ID) WHERE et.Entity_ID = ? AND (et.Type_ID = p.domain OR et.Type_ID = p.range) LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt1.setString(1, strs[1]);
                ResultSet res = stmt1.executeQuery();
                int id;
                if (res.next()) {
                    id = res.getInt("id");
                    stmt2.setInt(1, id);
                    t1 = System.currentTimeMillis();
                    stmt2.executeQuery();
                    t1 = System.currentTimeMillis() - t1;
                    t2 += t1;
                }
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
            PreparedStatement stmt1, stmt2;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt1 = conn.prepareStatement("SELECT Entity_ID id FROM Entity WHERE Entity_URI = ?");
                stmt2 = conn.prepareStatement("(SELECT map1.URI s, map2.URI o FROM FFF_Statement s JOIN idMap map1 ON (s.sID = map1.ID) JOIN idMap map2 ON (s.oID = map2.ID) WHERE (s.sID = ? OR s.oID = ?)) UNION " +
                        "(SELECT map.URI s, s.oValue o FROM FFN_Statement s JOIN idMap map ON (s.sID = map.ID) WHERE s.sID = ?) UNION " +
                        "(SELECT map1.URI s, map2.URI o FROM FNF_Statement s JOIN idMap map1 ON (s.sID = map1.ID) JOIN idMap map2 ON (s.oID = map2.ID) WHERE (s.sID = ? OR s.oID = ?)) UNION " +
                        "(SELECT map.URI s, s.oValue o FROM FNN_Statement s JOIN idMap map ON (s.sID = map.ID) WHERE s.sID = ?) LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt1.setString(1, strs[1]);
                ResultSet res = stmt1.executeQuery();
                int id;
                if (res.next()) {
                    id = res.getInt("id");
                    stmt2.setInt(1, id);
                    stmt2.setInt(2, id);
                    stmt2.setInt(3, id);
                    stmt2.setInt(4, id);
                    stmt2.setInt(5, id);
                    stmt2.setInt(6, id);

                    t1 = System.currentTimeMillis();
                    stmt2.executeQuery();
                    t1 = System.currentTimeMillis() - t1;
                    t2 += t1;
                }
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
            PreparedStatement stmt1, stmt2;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt1 = conn.prepareStatement("SELECT Type_ID id FROM Type WHERE Type_URI = ?");
                stmt2 = conn.prepareStatement("SELECT map.URI id FROM EntityType et JOIN idMap map ON (et.Entity_ID = map.ID) WHERE et.Type_ID = ? LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt1.setString(1, strs[1]);
                ResultSet res = stmt1.executeQuery();
                int id;
                if (res.next()) {
                    id = res.getInt("id");
                    stmt2.setInt(1, id);
                    t1 = System.currentTimeMillis();
                    stmt2.executeQuery();
                    t1 = System.currentTimeMillis() - t1;
                    t2 += t1;
                }
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
            PreparedStatement stmt1, stmt2;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt1 = conn.prepareStatement("SELECT Type_Domain `domain` FROM Type WHERE Type_URI = ?");
                stmt2 = conn.prepareStatement("SELECT map.URI id FROM Type t JOIN idMap map ON (t.Type_ID = map.ID) WHERE t.Type_Domain = ? LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt1.setString(1, strs[1]);
                ResultSet res = stmt1.executeQuery();
                int domain;
                if (res.next()) {
                    domain = res.getInt("domain");
                    stmt2.setInt(1, domain);
                    t1 = System.currentTimeMillis();
                    stmt2.executeQuery();
                    t1 = System.currentTimeMillis() - t1;
                    t2 += t1;
                }
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
            PreparedStatement stmt1, stmt2;
            long t1 = 0, t2 = 0;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                stmt1 = conn.prepareStatement("SELECT Property_ID id FROM Property WHERE Property_URI = ?");
                stmt2 = conn.prepareStatement("SELECT map1.URI s, map2.URI o FROM FFF_Statement s JOIN idMap map1 ON (s.sID = map1.ID) JOIN idMap map2 ON (s.oID = map2.ID) WHERE s.pID = ? LIMIT 0, 50");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] strs = line.split("\t");
                stmt1.setString(1, strs[1]);
                ResultSet res = stmt1.executeQuery();
                int id;
                if (res.next()) {
                    id = res.getInt("domain");
                    stmt2.setInt(1, id);
                    t1 = System.currentTimeMillis();
                    stmt2.executeQuery();
                    t1 = System.currentTimeMillis() - t1;
                    t2 += t1;
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                System.out.println(t2);
            }
        });
        reader.read(fname_property, Main.url);
    }
}
