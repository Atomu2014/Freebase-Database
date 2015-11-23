import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;

import java.io.*;
import java.sql.*;
import java.util.HashSet;

public class FileProcessing {
    private static String fname = "/media/kevin/D/database/sub-freebase";
    private static String url = "jdbc:mysql://localhost:3306/freebase?useServerPrepStmts=false&rewriteBatchedStatements=true&characterEncoding=utf8";
    private static String test_url = "jdbc:mysql://localhost:3306/new_schema?useServerPrepStmts=false&rewriteBatchedStatements=true&characterEncoding=utf8";

    private static boolean valid_ascii(String str) {
        for (int i = 0; i < str.length(); ++i) {
            if (str.charAt(i) >= 127) {
                return false;
            }
        }
        return true;
    }

    private static void insert_entity_type() {
        int nline = 0;
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fname), "utf-8"));
            FileWriter fw = new FileWriter("invalid_ascii_name");
            String line;

            long stime, etime;
            stime = System.currentTimeMillis();

            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, "root", "Kevin2015");
            conn.setAutoCommit(false);
//            PreparedStatement estmt = conn.prepareStatement("INSERT INTO Entity (Entity_URI) SELECT ? FROM DUAL " +
//                    "WHERE NOT EXISTS (SELECT Entity_URI FROM Entity WHERE Entity_URI = ?)");
//            PreparedStatement nstmt = conn.prepareStatement("UPDATE Entity SET name = ? WHERE Entity_URI = ?");
//            PreparedStatement tstmt = conn.prepareStatement("INSERT INTO Type (Type_URI) SELECT ? FROM DUAL " +
//                    "WHERE NOT EXISTS (SELECT Type_URI FROM Type WHERE Type_URI = ?)");
//            PreparedStatement etstmt = conn.prepareStatement("INSERT INTO EntityType (Entity_URI, Type_URI) SELECT ?, ? FROM DUAL " +
//                    "WHERE NOT EXISTS (SELECT Entity_URI, Type_URI FROM EntityType WHERE Entity_URI = ? AND Type_URI = ?)");

            int batchSize = 10000, timerSize = 1000000;

            while ((line = br.readLine()) != null) {
                nline++;
                String[] uris = line.split("\t");

//                estmt.setString(1, uris[0]);
//                estmt.setString(2, uris[0]);
//                estmt.addBatch();
//
//                if (uris[2].matches("<.*>")) {
//                    estmt.setString(1, uris[2]);
//                    estmt.setString(2, uris[2]);
//                    estmt.addBatch();
//                }

                if (uris[1].equals("<http://rdf.freebase.com/ns/type.object.type>")) {
//                    tstmt.setString(1, uris[2]);
//                    tstmt.setString(2, uris[2]);
//                    tstmt.addBatch();
//
//                    etstmt.setString(1, uris[0]);
//                    etstmt.setString(2, uris[2]);
//                    etstmt.setString(3, uris[0]);
//                    etstmt.setString(4, uris[2]);
//                    etstmt.addBatch();
                } else if (uris[1].equals("<http://rdf.freebase.com/ns/type.object.name>") && uris[2].matches(".*@en")) {
                    if (!valid_ascii(uris[2])) {
                        fw.write(uris[2] + "\n");
                    } else {
//                        nstmt.setString(1, uris[2]);
//                        nstmt.setString(2, uris[0]);
//                        nstmt.addBatch();
                    }
                }

//                if (nline % batchSize == 0) {
//                    estmt.executeBatch();
//                    tstmt.executeBatch();
//                    etstmt.executeBatch();
//                    nstmt.executeBatch();
//                    conn.commit();
//                }

                if (nline % timerSize == 0) {
                    etime = System.currentTimeMillis();
                    System.out.printf("" + nline + "\t" + (etime - stime) + "\n");
                    stime = etime;
                }
            }

//            if (nline % batchSize != 0) {
//                estmt.executeBatch();
//                tstmt.executeBatch();
//                etstmt.executeBatch();
//                nstmt.executeBatch();
//                conn.commit();
//            }

            conn.close();
            br.close();
            fw.close();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(nline);
        }
    }

    private static void insert_property() {
        int nline = 0;
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fname), "utf-8"));
            String line;

            long stime, etime;
            stime = System.currentTimeMillis();

            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, "root", "Kevin2015");
            conn.setAutoCommit(false);

            PreparedStatement tstmt = conn.prepareStatement("INSERT INTO Type (Type_URI) SELECT ? FROM DUAL " +
                    "WHERE NOT EXISTS (SELECT Type_URI FROM Type WHERE Type_URI = ?)");
            PreparedStatement rpstmt = conn.prepareStatement("INSERT INTO RelationProperty (Property_URI) SELECT ? FROM DUAL " +
                    "WHERE NOT EXISTS (SELECT Property_URI FROM RelationProperty WHERE Property_URI = ?)");
            PreparedStatement vpstmt = conn.prepareStatement("INSERT INTO ValueProperty (Property_URI) SELECT ? FROM DUAL " +
                    "WHERE NOT EXISTS (SELECT Property_URI FROM ValueProperty WHERE Property_URI = ?)");


            int batchSize = 10000, timerSize = 100000;

            while ((line = br.readLine()) != null) {
                nline++;
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

//                if (uris[1].equals("<http://rdf.freebase.com/ns/type.property.schema>")) {
//                    if (!uris[2].matches("<.*>")) {
//                        System.out.println("schema ");
//                    }
//                } else if (uris[1].equals("<http://rdf.freebase.com/ns/type.property.expected_type>")) {
//                    if (!uris[2].matches("<.*>")) {
//                    }
//                }


                if (nline % batchSize == 0) {
                    rpstmt.executeBatch();
                    vpstmt.executeBatch();
                    conn.commit();
                }

                if (nline % timerSize == 0) {
                    etime = System.currentTimeMillis();
                    System.out.printf("" + nline + "\t" + (etime - stime) + "\n");
                    stime = etime;
                }
            }

            if (nline % batchSize == 0) {
                rpstmt.executeBatch();
                vpstmt.executeBatch();
                conn.commit();
            }

            conn.close();
            br.close();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(nline);
        }
    }

    private static void insert_statement() {
        int nline = 0;
        String line = "";

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fname), "utf-8"));

            long stime, etime;
            stime = System.currentTimeMillis();

            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, "root", "Kevin2015");
            conn.setAutoCommit(false);

            PreparedStatement tdstmt = conn.prepareStatement("INSERT INTO Type (Type_URI) SELECT ? FROM DUAL " +
                    "WHERE NOT EXISTS (SELECT Type_URI FROM Type WHERE Type_URI = ? COLLATE utf8_bin)");

            PreparedStatement trstmt = conn.prepareStatement("INSERT INTO Type (Type_URI) SELECT ? FROM DUAL " +
                    "WHERE (NOT EXISTS (SELECT Type_URI FROM Type WHERE Type_URI = ? COLLATE utf8_bin)) AND " +
                    "(EXISTS (SELECT Property_URI FROM RelationProperty WHERE Property_URI = ? COLLATE utf8_bin))");

            PreparedStatement rpdstmt = conn.prepareStatement("UPDATE RelationProperty SET domain = ? WHERE Property_URI = ? COLLATE utf8_bin");

            PreparedStatement rprstmt = conn.prepareStatement("UPDATE RelationProperty SET `range` = ? WHERE Property_URI = ? COLLATE utf8_bin");
            PreparedStatement vpdstmt = conn.prepareStatement("UPDATE ValueProperty SET domain = ? WHERE Property_URI = ? COLLATE utf8_bin");
            PreparedStatement vprstmt = conn.prepareStatement("UPDATE ValueProperty SET `range` = ? WHERE Property_URI = ? COLLATE utf8_bin");

            PreparedStatement rsstmt = conn.prepareStatement("INSERT INTO RelationStatement (sURI, pURI, oURI) SELECT ?, ?, ? FROM DUAL ");
//            +
//                    "WHERE NOT EXISTS (SELECT sURI FROM RelationStatement WHERE sURI = ? COLLATE utf8_bin AND pURI = ? COLLATE utf8_bin AND oURI = ? COLLATE utf8_bin)");

            PreparedStatement vsstmt = conn.prepareStatement("INSERT INTO ValueStatement (sURI, pURI, oValue) SELECT ?, ?, ? FROM DUAL ");
//            +
//                    "WHERE NOT EXISTS (SELECT sURI FROM ValueStatement WHERE sURI = ? COLLATE utf8_bin AND pURI = ? COLLATE utf8_bin AND oValue = ? COLLATE utf8_bin)");

//            PreparedStatement rpquery = conn.prepareStatement("SELECT COUNT(Property_URI) FROM RelationProperty WHERE Property_URI = ?");
//            ResultSet res = null;

            int batchSize = 1000, timerSize = 1000;

            while ((line = br.readLine()) != null) {
                nline++;
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

                if (nline % timerSize == 0) {
                    etime = System.currentTimeMillis();
                    System.out.printf("" + nline + "\t" + (etime - stime) + "\n");
                    stime = etime;
                }
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

            conn.close();
            br.close();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } catch (MySQLSyntaxErrorException e) {
            e.printStackTrace();
            System.out.println(nline);
            System.out.println(line);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(nline);
            System.out.println(line);
        }
    }

    private static void stat() {
        int nline = 0;
        try {
            int lenEntity = 0, lenValue = 0, lenProperty = 0, lenName = 0;
            HashSet<String> prefices = new HashSet<String>();
            FileWriter fw = new FileWriter("prefix");

            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fname), "utf-8"));
            String line;

            long stime, etime;
            stime = System.currentTimeMillis();

            int timerSize = 1000000;

            while ((line = br.readLine()) != null) {
                nline++;
                String[] uris = line.split("\t");

                lenEntity = Math.max(lenEntity, uris[0].length());
                lenProperty = Math.max(lenProperty, uris[1].length());
                if (uris[2].matches("<.*>")) {
                    lenEntity = Math.max(lenEntity, uris[2].length());
                } else {
                    lenValue = Math.max(lenValue, uris[2].length());
                }
                if (uris[1].equals("<http://rdf.freebase.com/ns/type.object.name>")) {
                    lenName = Math.max(lenName, uris[1].length());
                }
                for (int i = 0; i < 3; ++i) {
                    if (uris[i].matches("<.*>")) {
                        String prefix = uris[i].substring(1, uris[i].length() - 2);
                        int index = prefix.lastIndexOf('/');
                        if (index >= 0 && index < prefix.length()) {
                            prefices.add(prefix.substring(0, index));
                        }
                    }
                }

                if (nline % timerSize == 0) {
                    etime = System.currentTimeMillis();
                    System.out.printf("" + nline + "\t" + (etime - stime) + "\n");
                    stime = etime;
                }
            }

            br.close();
            fw.write("len entity:\t" + lenEntity + "\nlen value:\t" + lenValue + "\nlen name:\t" + lenName + "\nlen property:\t" + lenProperty + "\n");
            for (String pre : prefices) {
                fw.write(pre + "\n");
            }
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void test_innodb_file_per_table(){
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(test_url, "root", "Kevin2015");
            conn.setAutoCommit(true);

            PreparedStatement stmt = conn.prepareStatement("INSERT INTO new_table VALUES (?)");

            for (int i=0; i<1000000; ++i){
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
//        insert_entity_type();
//        stat();
//        insert_property();
//        insert_statement();

        test_innodb_file_per_table();
    }
}