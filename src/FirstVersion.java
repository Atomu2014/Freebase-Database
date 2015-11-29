import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FirstVersion {
    public static String fname_type = "/media/kevin/D/database/type";
    public static String fname_property = "/media/kevin/D/database/property";
    public static String fname_entity_type = "/media/kevin/D/database/entity_type";
    public static String fname_entity = "/media/kevin/D/database/entity";
    public static String fname_rstate = "/media/kevin/D/database/rfreebase";
    public static String fname_vstate = "/media/kevin/D/database/vfreebase";

    public static void insert_first_version_origin() {
        final int batchSize = 10000, timerSize = 100000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            String fb = "<http://rdf.freebase.com/ns/(.*)>";
            PreparedStatement rs_stmt, vs_stmt;
            PreparedStatement vp_stmt, vpd_stmt, vpr_stmt;
            PreparedStatement rp_stmt, rpd_stmt, rpr_stmt;
            PreparedStatement e_stmt, en_stmt, t_stmt, et_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException {
                rs_stmt = conn.prepareStatement("INSERT INTO RelationStatement VALUES (?, ?, ?)");

                vs_stmt = conn.prepareStatement("INSERT INTO ValueStatement VALUES (?, ?, ?)");

                vp_stmt = conn.prepareStatement("INSERT INTO ValueProperty (Property_URI) SELECT ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Property_URI FROM ValueProperty WHERE Property_URI = ?)");

                vpd_stmt = conn.prepareStatement("UPDATE ValueProperty SET `domain` = ? WHERE Property_URI = ?");
                vpr_stmt = conn.prepareStatement("UPDATE ValueProperty SET `range` = ? WHERE Property_URI = ?");

                rp_stmt = conn.prepareStatement("INSERT INTO RelationProperty (Property_URI) SELECT ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Property_URI FROM RelationProperty WHERE Property_URI = ?)");
                rpd_stmt = conn.prepareStatement("UPDATE RelationProperty SET `domain` = ? WHERE Property_URI = ?");
                rpr_stmt = conn.prepareStatement("UPDATE RelationProperty SET `range` = ? WHERE Property_URI = ?");

                e_stmt = conn.prepareStatement("INSERT INTO Entity (Entity_ID) SELECT ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Entity_ID FROM Entity WHERE Entity_ID = ?)");

                en_stmt = conn.prepareStatement("UPDATE Entity SET name = ? WHERE Entity_ID = ?");
                t_stmt = conn.prepareStatement("INSERT INTO Type (Type_URI) SELECT ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Type_URI FROM Type WHERE Type_URI = ?)");
                et_stmt = conn.prepareStatement("INSERT INTO EntityType VALUES (?, ?)");//" FROM DUAL " +
//                    "WHERE NOT EXISTS (SELECT Entity_ID FROM EntityType WHERE Entity_ID = ? AND Type_URI = ?)");

            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException {
                String[] uris = line.split("\t");
                uris[0] = "FB/" + uris[0].substring(28, uris[0].length() - 1);

                if (uris[2].matches(fb)) {
                    uris[2] = "FB/" + uris[2].substring(28, uris[2].length() - 1);

                    if (uris[1].matches(fb)) {
                        uris[1] = "FB/" + uris[1].substring(28, uris[1].length() - 1);
                        rp_stmt.setString(1, uris[1]);
                        rp_stmt.setString(2, uris[1]);
                        rp_stmt.addBatch();

                        if (uris[1].equals("FB/type.object.type")) {
                            e_stmt.setString(1, uris[0]);
                            e_stmt.setString(2, uris[0]);
                            e_stmt.addBatch();

                            t_stmt.setString(1, uris[2]);
                            t_stmt.setString(2, uris[2]);
                            t_stmt.addBatch();

                            et_stmt.setString(1, uris[0]);
                            et_stmt.setString(2, uris[2]);
//                            et_stmt.setString(3, uris[0]);
//                            et_stmt.setString(4, uris[2]);
                            et_stmt.addBatch();
                        } else if (uris[1].equals("FB/type.type.instance")) {
                            e_stmt.setString(1, uris[2]);
                            e_stmt.setString(2, uris[2]);
                            e_stmt.addBatch();

                            t_stmt.setString(1, uris[0]);
                            t_stmt.setString(2, uris[0]);
                            t_stmt.addBatch();

//                            et_stmt.setString(1, uris[2]);
//                            et_stmt.setString(2, uris[0]);
//                            et_stmt.setString(3, uris[2]);
//                            et_stmt.setString(4, uris[0]);
//                            et_stmt.addBatch();
                        }
                    }

                    rs_stmt.setString(1, uris[0]);
                    rs_stmt.setString(2, uris[1]);
                    rs_stmt.setString(3, uris[2]);
                    rs_stmt.addBatch();
                } else {
                    if (uris[1].matches(fb)) {
                        uris[1] = "FB/" + uris[1].substring(28, uris[1].length() - 1);
                        vp_stmt.setString(1, uris[1]);
                        vp_stmt.setString(2, uris[1]);
                        vp_stmt.addBatch();

                        if (uris[1].equals("FB/type.object.name") && uris[2].matches(".*@en")) {
                            en_stmt.setString(1, uris[2]);
                            en_stmt.setString(2, uris[0]);
                            en_stmt.addBatch();
                        }
                    }

                    vs_stmt.setString(1, uris[0]);
                    vs_stmt.setString(2, uris[1]);
                    vs_stmt.setString(3, uris[2]);
                    vs_stmt.addBatch();
                }

                // TODO: should insert property
                if (uris[1].equals("FB/type.property.schema")) {
                    rpd_stmt.setString(1, uris[2]);
                    rpd_stmt.setString(2, uris[0]);
                    rpd_stmt.addBatch();

                    vpd_stmt.setString(1, uris[2]);
                    vpd_stmt.setString(2, uris[0]);
                    vpd_stmt.addBatch();
                } else if (uris[1].equals("FB/type.property.expected_type")) {
                    rpr_stmt.setString(1, uris[2]);
                    rpr_stmt.setString(2, uris[0]);
                    rpr_stmt.addBatch();

                    vpr_stmt.setString(1, uris[2]);
                    vpr_stmt.setString(2, uris[0]);
                    vpr_stmt.addBatch();
                }


                if (nline % batchSize == 0) {
                    rs_stmt.executeBatch();
                    vs_stmt.executeBatch();
                    vp_stmt.executeBatch();
                    vpd_stmt.executeBatch();
                    vpr_stmt.executeBatch();
                    rp_stmt.executeBatch();
                    rpd_stmt.executeBatch();
                    rpr_stmt.executeBatch();
                    e_stmt.executeBatch();
                    en_stmt.executeBatch();
                    t_stmt.executeBatch();
                    et_stmt.executeBatch();
                    conn.commit();
                }

            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException {
                if (nline % batchSize != 0) {
                    rs_stmt.executeBatch();
                    vs_stmt.executeBatch();
                    vp_stmt.executeBatch();
                    vpd_stmt.executeBatch();
                    vpr_stmt.executeBatch();
                    rp_stmt.executeBatch();
                    rpd_stmt.executeBatch();
                    rpr_stmt.executeBatch();
                    e_stmt.executeBatch();
                    en_stmt.executeBatch();
                    t_stmt.executeBatch();
                    et_stmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(Main.fname, Main.url);

    }

    public static void insert_type() {
        final int batchSize = 10000, timerSize = 100000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement t_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException {
                t_stmt = conn.prepareStatement("INSERT INTO Type VALUES (?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException {
                t_stmt.setString(1, line);
                t_stmt.addBatch();

                if (nline % batchSize == 0) {
                    t_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException {
                if (nline % batchSize != 0) {
                    t_stmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(fname_type, Main.url);
    }

    public static void insert_property() {
        final int batchSize = 10000, timerSize = 100000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement p_stmt, pd_stmt, pr_stmt, pdr_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                p_stmt = conn.prepareStatement("INSERT INTO Property(Property_URI) VALUES (?)");
                pd_stmt = conn.prepareStatement("INSERT INTO Property(Property_URI, `domain`) VALUES (?, ?)");
                pr_stmt = conn.prepareStatement("INSERT INTO Property(Property_URI, `range`) VALUES (?, ?)");
                pdr_stmt = conn.prepareStatement("INSERT INTO Property VALUES (?, ?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                if (uris[1].equals("null")) {
                    if (uris[2].equals("null")) {
                        p_stmt.setString(1, uris[0]);
                        p_stmt.addBatch();
                    } else {
                        pr_stmt.setString(1, uris[0]);
                        pr_stmt.setString(2, uris[2]);
                        pr_stmt.addBatch();
                    }
                } else if (uris[2].equals("null")) {
                    pd_stmt.setString(1, uris[0]);
                    pd_stmt.setString(2, uris[1]);
                    pd_stmt.executeBatch();
                } else {
                    pdr_stmt.setString(1, uris[0]);
                    pdr_stmt.setString(2, uris[1]);
                    pdr_stmt.setString(3, uris[2]);
                    pdr_stmt.addBatch();
                }

                if (nline % batchSize == 0) {
                    p_stmt.executeBatch();
                    pd_stmt.executeBatch();
                    pr_stmt.executeBatch();
                    pdr_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    p_stmt.executeBatch();
                    pd_stmt.executeBatch();
                    pr_stmt.executeBatch();
                    pdr_stmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(fname_property, Main.url);
    }

    public static void insert_entity_type() {
        final int batchSize = 10000, timerSize = 1000000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement et_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                et_stmt = conn.prepareStatement("INSERT INTO EntityType VALUES (?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");

                et_stmt.setString(1, uris[0]);
                et_stmt.setString(2, uris[1]);
                et_stmt.addBatch();

                if (nline % batchSize == 0) {
                    et_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    et_stmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(fname_entity_type, Main.url);

    }

    public static void insert_entity() {
        final int batchSize = 10000, timerSize = 1000000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement e_stmt, en_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                e_stmt = conn.prepareStatement("INSERT INTO Entity(Entity_ID) VALUES (?)");
                en_stmt = conn.prepareStatement("INSERT INTO Entity VALUES (?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                if (uris.length > 1) {
                    en_stmt.setString(1, uris[0]);
                    en_stmt.setString(2, uris[1]);
                    en_stmt.addBatch();
                } else {
                    e_stmt.setString(1, uris[0]);
                    e_stmt.addBatch();
                }

                if (nline % batchSize == 0) {
                    e_stmt.executeBatch();
                    en_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    e_stmt.executeBatch();
                    en_stmt.executeBatch();
                    conn.commit();
                }
            }
        });

        reader.read(fname_entity, Main.url);
    }

    public static void insert_rstste() {
        final int batchSize = 10000, timerSize = 1000000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement rs_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                rs_stmt = conn.prepareStatement("INSERT INTO RelationStatement VALUES (?, ?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                rs_stmt.setString(1, uris[0]);
                rs_stmt.setString(2, uris[1]);
                rs_stmt.setString(3, uris[2]);
                rs_stmt.addBatch();

                if (nline % batchSize == 0) {
                    rs_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    rs_stmt.executeBatch();
                    conn.commit();
                }
            }
        });

        reader.read(fname_rstate, Main.url);
    }

    public static void insert_vstste() {
        final int batchSize = 10000, timerSize = 1000000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement vs_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                vs_stmt = conn.prepareStatement("INSERT INTO ValueStatement VALUES (?, ?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                vs_stmt.setString(1, uris[0]);
                vs_stmt.setString(2, uris[1]);
                vs_stmt.setString(3, uris[2]);
                vs_stmt.addBatch();

                if (nline % batchSize == 0) {
                    vs_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    vs_stmt.executeBatch();
                    conn.commit();
                }
            }
        });

        reader.read(fname_vstate, Main.url);
    }
}
