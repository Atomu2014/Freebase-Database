import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Raw {
    public static boolean valid_ascii(String str) {
        for (int i = 0; i < str.length(); ++i) {
            if (str.charAt(i) >= 127) {
                return false;
            }
        }
        return true;
    }

    public static void insert_entity_type() {
        final int batchSize = 10000, timerSize = 1000000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            FileWriter fw;
            PreparedStatement estmt, nstmt, tstmt, etstmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                fw = new FileWriter("invalid_ascii_name");

                estmt = conn.prepareStatement("INSERT INTO Entity (Entity_URI) SELECT ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Entity_URI FROM Entity WHERE Entity_URI = ?)");
                nstmt = conn.prepareStatement("UPDATE Entity SET name = ? WHERE Entity_URI = ?");
                tstmt = conn.prepareStatement("INSERT INTO Type (Type_URI) SELECT ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Type_URI FROM Type WHERE Type_URI = ?)");
                etstmt = conn.prepareStatement("INSERT INTO EntityType (Entity_URI, Type_URI) SELECT ?, ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Entity_URI, Type_URI FROM EntityType WHERE Entity_URI = ? AND Type_URI = ?)");

            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");

                estmt.setString(1, uris[0]);
                estmt.setString(2, uris[0]);
                estmt.addBatch();

                if (uris[2].matches("<.*>")) {
                    estmt.setString(1, uris[2]);
                    estmt.setString(2, uris[2]);
                    estmt.addBatch();
                }

                if (uris[1].equals("<http://rdf.freebase.com/ns/type.object.type>")) {
                    tstmt.setString(1, uris[2]);
                    tstmt.setString(2, uris[2]);
                    tstmt.addBatch();

                    etstmt.setString(1, uris[0]);
                    etstmt.setString(2, uris[2]);
                    etstmt.setString(3, uris[0]);
                    etstmt.setString(4, uris[2]);
                    etstmt.addBatch();
                } else if (uris[1].equals("<http://rdf.freebase.com/ns/type.object.name>") && uris[2].matches(".*@en")) {
                    if (!valid_ascii(uris[2])) {
                        fw.write(uris[2] + "\n");
                    } else {
                        nstmt.setString(1, uris[2]);
                        nstmt.setString(2, uris[0]);
                        nstmt.addBatch();
                    }
                }

                if (nline % batchSize == 0) {
                    estmt.executeBatch();
                    tstmt.executeBatch();
                    etstmt.executeBatch();
                    nstmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    estmt.executeBatch();
                    tstmt.executeBatch();
                    etstmt.executeBatch();
                    nstmt.executeBatch();
                    conn.commit();
                }

                fw.close();
            }

        });
        reader.read(Main.fname, Main.url);
    }

    public static void insert_property() {
        final int batchSize = 10000, timerSize = 100000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement tstmt, rpstmt, vpstmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                tstmt = conn.prepareStatement("INSERT INTO Type (Type_URI) SELECT ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Type_URI FROM Type WHERE Type_URI = ?)");
                rpstmt = conn.prepareStatement("INSERT INTO RelationProperty (Property_URI) SELECT ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Property_URI FROM RelationProperty WHERE Property_URI = ?)");
                vpstmt = conn.prepareStatement("INSERT INTO ValueProperty (Property_URI) SELECT ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Property_URI FROM ValueProperty WHERE Property_URI = ?)");

            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");

                if (uris[2].matches("<.*>")) {
                    rpstmt.setString(1, uris[1]);
                    rpstmt.setString(2, uris[1]);
                    rpstmt.addBatch();
                } else {
                    vpstmt.setString(1, uris[1]);
                    vpstmt.setString(2, uris[1]);
                    vpstmt.addBatch();
                }

                if (uris[1].equals("<http://rdf.freebase.com/ns/type.property.schema>")) {
                    if (!uris[2].matches("<.*>")) {
                        System.out.println("schema ");
                    }
                } else if (uris[1].equals("<http://rdf.freebase.com/ns/type.property.expected_type>")) {
                    if (!uris[2].matches("<.*>")) {
                    }
                }


                if (nline % batchSize == 0) {
                    rpstmt.executeBatch();
                    vpstmt.executeBatch();
                    conn.commit();
                }

            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    rpstmt.executeBatch();
                    vpstmt.executeBatch();
                    conn.commit();
                }
            }
        });
        reader.read(Main.fname, Main.url);

    }

    public static void insert_statement() {
        final int batchSize = 1000, timerSize = 1000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            PreparedStatement tdstmt, trstmt, rpdstmt, rprstmt, vpdstmt, vprstmt, rsstmt, vsstmt;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {

                tdstmt = conn.prepareStatement("INSERT INTO Type (Type_URI) SELECT ? FROM DUAL " +
                        "WHERE NOT EXISTS (SELECT Type_URI FROM Type WHERE Type_URI = ? COLLATE utf8_bin)");

                trstmt = conn.prepareStatement("INSERT INTO Type (Type_URI) SELECT ? FROM DUAL " +
                        "WHERE (NOT EXISTS (SELECT Type_URI FROM Type WHERE Type_URI = ? COLLATE utf8_bin)) AND " +
                        "(EXISTS (SELECT Property_URI FROM RelationProperty WHERE Property_URI = ? COLLATE utf8_bin))");

                rpdstmt = conn.prepareStatement("UPDATE RelationProperty SET domain = ? WHERE Property_URI = ? COLLATE utf8_bin");

                rprstmt = conn.prepareStatement("UPDATE RelationProperty SET `range` = ? WHERE Property_URI = ? COLLATE utf8_bin");
                vpdstmt = conn.prepareStatement("UPDATE ValueProperty SET domain = ? WHERE Property_URI = ? COLLATE utf8_bin");
                vprstmt = conn.prepareStatement("UPDATE ValueProperty SET `range` = ? WHERE Property_URI = ? COLLATE utf8_bin");

                rsstmt = conn.prepareStatement("INSERT INTO RelationStatement (sURI, pURI, oURI) SELECT ?, ?, ? FROM DUAL ");
//            +
//                    "WHERE NOT EXISTS (SELECT sURI FROM RelationStatement WHERE sURI = ? COLLATE utf8_bin AND pURI = ? COLLATE utf8_bin AND oURI = ? COLLATE utf8_bin)");

                vsstmt = conn.prepareStatement("INSERT INTO ValueStatement (sURI, pURI, oValue) SELECT ?, ?, ? FROM DUAL ");
//            +
//                    "WHERE NOT EXISTS (SELECT sURI FROM ValueStatement WHERE sURI = ? COLLATE utf8_bin AND pURI = ? COLLATE utf8_bin AND oValue = ? COLLATE utf8_bin)");

//            PreparedStatement rpquery = conn.prepareStatement("SELECT COUNT(Property_URI) FROM RelationProperty WHERE Property_URI = ?");
//            ResultSet res = null;
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
                String[] uris = line.split("\t");

                if (uris[2].matches("<.*>")) {
                    rsstmt.setString(1, uris[0]);
                    rsstmt.setString(2, uris[1]);
                    rsstmt.setString(3, uris[2]);
//                    rsstmt.setString(4, uris[0]);
//                    rsstmt.setString(5, uris[1]);
//                    rsstmt.setString(6, uris[2]);
                    rsstmt.addBatch();
                } else {
                    vsstmt.setString(1, uris[0]);
                    vsstmt.setString(2, uris[1]);
                    vsstmt.setString(3, uris[2]);
//                    vsstmt.setString(4, uris[0]);
//                    vsstmt.setString(5, uris[1]);
//                    vsstmt.setString(6, uris[2]);
                    vsstmt.addBatch();
                }

                if (uris[1].equals("<http://rdf.freebase.com/ns/type.property.schema>")) {
                    tdstmt.setString(1, uris[2]);
                    tdstmt.setString(2, uris[2]);
                    tdstmt.addBatch();

                    rpdstmt.setString(1, uris[2]);
                    rpdstmt.setString(2, uris[0]);
                    rpdstmt.addBatch();

                    vpdstmt.setString(1, uris[2]);
                    vpdstmt.setString(2, uris[0]);
                    vpdstmt.addBatch();
                } else if (uris[1].equals("<http://rdf.freebase.com/ns/type.property.expected_type>")) {
                    trstmt.setString(1, uris[2]);
                    trstmt.setString(2, uris[2]);
                    trstmt.setString(3, uris[2]);
                    trstmt.addBatch();

                    rprstmt.setString(1, uris[2]);
                    rprstmt.setString(2, uris[0]);
                    rprstmt.addBatch();

                    vprstmt.setString(1, uris[2]);
                    vprstmt.setString(2, uris[0]);
                    vprstmt.addBatch();
                }


                if (nline % batchSize == 0) {
                    tdstmt.executeBatch();
                    trstmt.executeBatch();
                    rpdstmt.executeBatch();
                    rprstmt.executeBatch();
                    vpdstmt.executeBatch();
                    vprstmt.executeBatch();
                    rsstmt.executeBatch();
                    vsstmt.executeBatch();
                    conn.commit();
                }
            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                if (nline % batchSize != 0) {
                    tdstmt.executeBatch();
                    trstmt.executeBatch();
                    rpdstmt.executeBatch();
                    rprstmt.executeBatch();
                    vpdstmt.executeBatch();
                    vprstmt.executeBatch();
                    rsstmt.executeBatch();
                    vsstmt.executeBatch();
                    conn.commit();
                }
            }

        });
        reader.read(Main.fname, Main.url);
    }
}
