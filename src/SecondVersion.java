import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SecondVersion {

    public static String fname_type = "/media/kevin/D/database/type_2";
    public static String fname_property = "/media/kevin/D/database/property_2";
    public static String fname_entity_type = "/media/kevin/D/database/entity_type_2";
    public static String fname_entity = "/media/kevin/D/database/entity_2";
    public static String fname_fff = "/media/kevin/D/database/freebase_fff_2";
    public static String fname_ffn = "/media/kevin/D/database/freebase_ffn_2";
    public static String fname_fnf = "/media/kevin/D/database/freebase_fnf_2";
    public static String fname_fnn = "/media/kevin/D/database/freebase_fnn_2";
    public static String fname_id_map = "/media/kevin/D/database/idMap";
    public static int batchSize = 10000, timerSize = 1000000;

    public static void insert_type() {
        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement t_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                t_stmt = conn.prepareStatement("INSERT INTO Type VALUES (?, ?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                t_stmt.setInt(1, Integer.parseInt(uris[0]));
                t_stmt.setString(2, uris[1]);
                t_stmt.setInt(3, Integer.parseInt(uris[2]));
                t_stmt.addBatch();

                if (nline % batchSize == 0) {
                    t_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    t_stmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(fname_type, Main.url);
    }

    public static void insert_property() {
        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement p_stmt, pd_stmt, pr_stmt, pdr_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                p_stmt = conn.prepareStatement("INSERT INTO Property(Property_ID, Property_URI) VALUES (?, ?)");
                pd_stmt = conn.prepareStatement("INSERT INTO Property(Property_ID, Property_URI, `domain`) VALUES (?, ?, ?)");
                pr_stmt = conn.prepareStatement("INSERT INTO Property(Property_ID, Property_URI, `range`) VALUES (?, ?, ?)");
                pdr_stmt = conn.prepareStatement("INSERT INTO Property VALUES (?, ?, ?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                if (uris[2].equals("null")) {
                    if (uris[3].equals("null")) {
                        p_stmt.setInt(1, Integer.parseInt(uris[0]));
                        p_stmt.setString(2, uris[1]);
                        p_stmt.addBatch();
                    } else {
                        pr_stmt.setInt(1, Integer.parseInt(uris[0]));
                        pr_stmt.setString(2, uris[1]);
                        pr_stmt.setInt(3, Integer.parseInt(uris[3]));
                        pr_stmt.addBatch();
                    }
                } else if (uris[3].equals("null")) {
                    pd_stmt.setInt(1, Integer.parseInt(uris[0]));
                    pd_stmt.setString(2, uris[1]);
                    pd_stmt.setInt(3, Integer.parseInt(uris[2]));
                    pd_stmt.executeBatch();
                } else {
                    pdr_stmt.setInt(1, Integer.parseInt(uris[0]));
                    pdr_stmt.setString(2, uris[1]);
                    pdr_stmt.setInt(3, Integer.parseInt(uris[2]));
                    pdr_stmt.setInt(4, Integer.parseInt(uris[3]));
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

    public static void insert_entity() {
        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement e_stmt, en_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                e_stmt = conn.prepareStatement("INSERT INTO Entity(Entity_ID, Entity_URI) VALUES (?, ?)");
                en_stmt = conn.prepareStatement("INSERT INTO Entity VALUES (?, ?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                if (uris[2].equals("null")) {
                    e_stmt.setInt(1, Integer.parseInt(uris[0]));
                    e_stmt.setString(2, uris[1]);
                    e_stmt.addBatch();
                } else {
                    en_stmt.setInt(1, Integer.parseInt(uris[0]));
                    en_stmt.setString(2, uris[1]);
                    en_stmt.setString(3, uris[2]);
                    en_stmt.addBatch();
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

    public static void insert_entity_type() {
        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement et_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                et_stmt = conn.prepareStatement("INSERT INTO EntityType VALUES (?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");

                et_stmt.setInt(1, Integer.parseInt(uris[0]));
                et_stmt.setInt(2, Integer.parseInt(uris[1]));
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

    public static void insert_fff() {
        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement fff_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                fff_stmt = conn.prepareStatement("INSERT INTO FFF_Statement VALUES (?, ?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                fff_stmt.setInt(1, Integer.parseInt(uris[0]));
                fff_stmt.setInt(2, Integer.parseInt(uris[1]));
                fff_stmt.setInt(3, Integer.parseInt(uris[2]));
                fff_stmt.addBatch();

                if (nline % batchSize == 0) {
                    fff_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    fff_stmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(fname_fff, Main.url);
    }

    public static void insert_ffn() {
        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement ffn_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                ffn_stmt = conn.prepareStatement("INSERT INTO FFN_Statement VALUES (?, ?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                ffn_stmt.setInt(1, Integer.parseInt(uris[0]));
                ffn_stmt.setInt(2, Integer.parseInt(uris[1]));
                ffn_stmt.setString(3, uris[2]);
                ffn_stmt.addBatch();

                if (nline % batchSize == 0) {
                    ffn_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    ffn_stmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(fname_ffn, Main.url);
    }

    public static void insert_fnf() {
        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement fnf_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                fnf_stmt = conn.prepareStatement("INSERT INTO FNF_Statement VALUES (?, ?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                fnf_stmt.setInt(1, Integer.parseInt(uris[0]));
                fnf_stmt.setString(2, uris[1]);
                fnf_stmt.setInt(3, Integer.parseInt(uris[2]));
                fnf_stmt.addBatch();

                if (nline % batchSize == 0) {
                    fnf_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    fnf_stmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(fname_fnf, Main.url);
    }

    public static void insert_fnn() {
        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement fnn_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                fnn_stmt = conn.prepareStatement("INSERT INTO FNN_Statement VALUES (?, ?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                fnn_stmt.setInt(1, Integer.parseInt(uris[0]));
                fnn_stmt.setString(2, uris[1]);
                fnn_stmt.setString(3, uris[2]);
                fnn_stmt.addBatch();

                if (nline % batchSize == 0) {
                    fnn_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    fnn_stmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(fname_fnn, Main.url);
    }

    public static void insert_id_map() {
        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement id_stmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                id_stmt = conn.prepareStatement("INSERT INTO idMap VALUES (?, ?)");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");
                id_stmt.setInt(1, Integer.parseInt(uris[1]));
                id_stmt.setString(2, uris[0]);
                id_stmt.addBatch();

                if (nline % batchSize == 0){
                    id_stmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0){
                    id_stmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(fname_id_map, Main.url);
    }
}
