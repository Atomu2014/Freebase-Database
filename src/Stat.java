import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;

public class Stat {

    public static void stat() {
        final int timerSize = 1000000;

        FBFileReader reader = new FBFileReader(timerSize, new onProcessListener() {
            int lenEntity = 0, lenValue = 0, lenProperty = 0, lenName = 0;
            HashSet<String> prefices = new HashSet<String>();
            FileWriter fw;

            @Override
            public void onPrepare(Connection conn) throws SQLException, IOException {
                fw = new FileWriter("prefix");
            }

            @Override
            public void onWhile(Connection conn, int nline, String line) throws SQLException, IOException {
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

            }

            @Override
            public void onFinish(Connection conn, int nline) throws SQLException, IOException {
                fw.write("len entity:\t" + lenEntity + "\nlen value:\t" + lenValue + "\nlen name:\t" + lenName + "\nlen property:\t" + lenProperty + "\n");
                for (String pre : prefices) {
                    fw.write(pre + "\n");
                }
                fw.close();
            }

        });
    }

}
