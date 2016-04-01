import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.PrintWriter;
import java.net.Socket;

import java.util.*; 
import java.net.*;                                                      
import java.io.*; 

public class fileServerListener implements Runnable {

  private String[] dP;
  private Socket socket;
  private ObjectOutputStream toWorker;
  private ObjectInputStream fromWorker;

  public fileServerListener(Socket socket, String[] dP) {
    this.dP = dP;
    this.socket = socket;
  }

  public void run() {
    
    while (true) {
      try {
        toWorker = new ObjectOutputStream(socket.getOutputStream());
        fromWorker = new ObjectInputStream(socket.getInputStream());
        //Read socket input
        int id = (int) fromWorker.readObject();
        System.out.println("fileServerListener received dictionary request for partition: "+id);
        //Get Correct String partition
        String partition = dP[id];
        //Send back partition data
        toWorker.writeObject(partition);
        //System.out.println("fileServerListener sent data:\n"+partition);
      } catch (IOException e){
        e.printStackTrace();
        break;
      } catch (Exception e) {
        System.out.println("fileServerListener Make node:" + e.getMessage());
      }
    }
  }


}
