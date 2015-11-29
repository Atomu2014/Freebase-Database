import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by kevin on 15-11-26.
 */
public class FBFileReader {
    public int nline;
    public String line;
    public int timerSize;
    private onProcessListener listener;

    public FBFileReader(int ts, onProcessListener l) {
        timerSize = ts;
        listener = l;
    }

    public void read(String fname, String dbpath) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fname), "utf-8"));
            Class.forName("com.mysql.jdbc.Driver");

//            Connection conn = DriverManager.getConnection(dbpath, "qyr", "qyr123456");
            Connection conn = DriverManager.getConnection(dbpath, "root", "Kevin2015");
            conn.setAutoCommit(false);

            nline = 0;
            listener.onPrepare(conn);

            long stime, etime;
            stime = System.currentTimeMillis();

            while ((line = br.readLine()) != null) {
                nline++;

                listener.onWhile(conn, nline, line);

                if (nline % timerSize == 0) {
                    etime = System.currentTimeMillis();
                    System.out.printf("" + nline + "\t" + (etime - stime) + "\n");
                    stime = etime;
                }
            }

            listener.onFinish(conn, nline);

            conn.close();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
