import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.PrintWriter;
import java.net.Socket;

public class fileServerListener implements Runnable {

  private String[] dP;
  private Socket socket;

  public fileServerListener(Socket socket, String[] dP) {
    this.dP = dP;
    this.socket = socket;
  }

  public void run() {

    while (true) {
      try {
        //Read socket input
        DataInputStream inData = new DataInputStream(socket.getInputStream());
        //Convert to Int
        int id = inData.readInt();
        //Get Correct String partition
        String partition = dP[id];
        //Send back partition data
        OutputStream oS = socket.getOutputStream();
        PrintWriter out = new PrintWriter(oS);
        out.print(partition);
      } catch (IOException e){
        e.printStackTrace();
      }
    }
  }


}
