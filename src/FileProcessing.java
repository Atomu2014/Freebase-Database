import java.io.*;
import java.sql.*;

public class FileProcessing {
    private static String fname = "/media/kevin/D/database/sub-freebase";
    private static String url = "jdbc:mysql://localhost:3306/freebase?useServerPrepStmts=false&rewriteBatchedStatements=true&characterEncoding=utf8";

    private static void insert_entity_type() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(fname));

            String line = null;
            int nline = 0;
            long stime, etime;
            stime = System.currentTimeMillis();

            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, "root", "Kevin2015");
            conn.setAutoCommit(false);
            PreparedStatement estmt = conn.prepareStatement("INSERT INTO Entity (Entity_URI) SELECT ? FROM DUAL " +
                    "WHERE NOT EXISTS (SELECT Entity_URI FROM Entity WHERE Entity_URI = ?)");
            PreparedStatement tstmt = conn.prepareStatement("INSERT INTO Type (Type_URI) SELECT ? FROM DUAL " +
                    "WHERE NOT EXISTS (SELECT Type_URI FROM Type WHERE Type_URI = ?)");
            PreparedStatement etstmt = conn.prepareStatement("INSERT INTO EntityType (Entity_URI, Type_URI) SELECT ?, ? FROM DUAL " +
                    "WHERE NOT EXISTS (SELECT Entity_URI, Type_URI FROM EntityType WHERE Entity_URI = ? AND Type_URI = ?)");

            while ((line = br.readLine()) != null) {
                nline++;
                String[] uris = line.split("\t");

//                estmt.setString(1, uris[0]);
//                estmt.setString(2, uris[0]);
//                estmt.addBatch();
//
//                tstmt.setString(1, uris[1]);
//                tstmt.setString(2, uris[1]);
//                tstmt.addBatch();
//
//                if (uris[2].matches("<.*>")) {
//                    estmt.setString(1, uris[2]);
//                    estmt.setString(2, uris[2]);
//                    estmt.addBatch();
//                }
//
//                if (nline % 1000 == 0) {
//                    estmt.executeBatch();
//                    tstmt.executeBatch();
//                    conn.commit();
//                }

                etstmt.setString(1, uris[0]);
                etstmt.setString(2, uris[1]);
                etstmt.setString(3, uris[0]);
                etstmt.setString(4, uris[1]);
                etstmt.addBatch();

                if (nline % 10000 == 0) {
                    etstmt.executeBatch();
                    conn.commit();
                }

                if (nline % 1000000 == 0) {
                    etime = System.currentTimeMillis();
                    System.out.printf("" + nline + "\t" + (etime - stime) + "\n");
                    stime = etime;
                }
            }

            if (nline % 10000 != 0) {
//                estmt.executeBatch();
//                tstmt.executeBatch();
                etstmt.executeBatch();
                conn.commit();
            }

//            System.out.printf("" + nline + "\n" + max_len + "\n");
            conn.close();
            br.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void stat() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(fname));

            String line;
            int nline = 0;
            long stime, etime;
            stime = System.currentTimeMillis();

            int max_len = -1;

            while ((line = br.readLine()) != null) {
                nline++;
                String[] uris = line.split("\t");

                if (!uris[2].matches("<.*>")) {
                    max_len = Math.max(max_len, uris[2].length());
                }

                if (nline % 1000000 == 0) {
                    etime = System.currentTimeMillis();
                    System.out.printf("" + nline + "\t" + (etime - stime) + "\n");
                    stime = etime;
                }
            }

            System.out.printf("" + nline + "\t" + max_len + "\n");

            br.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //insert_entity_type();
        stat();
    }
}