import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;
import java.io.FileReader;
import java.io.BufferedReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.net.UnknownHostException;

public class fileServer {

  public String myPath = "/fileServer";
  public String dictionaryPath = "dictionary/lowercase.rand";
  public ZkConnector zkc;
  public Watcher watcher;
  public String myHost = "";
  public String[] dP = new String[27]; //10000 words per entry
  
  //Defines
  public final int MAX_WORD_LIMIT = 10000;

  public static void main(String[] args) throws IOException {

    
    //Require 2 arguments
    //  arg1: zkServer:serverPort
    if (args.length !=1 ) {
      System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. fileServer zkServer:serverPort");
      return;
    }


    ServerSocket sS = new ServerSocket(0);

    //Get localhost name and port
    InetAddress ip;
    String myHost = "";
    try {
      ip = InetAddress.getLocalHost();
      myHost = ip.getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    int myPort = sS.getLocalPort();

    System.out.println("hostName:hostPort = "+myHost+":"+myPort);

    fileServer fs = new fileServer(args[0], myHost+":"+myPort);

    //System.out.println("Sleeping...");
    try {
      Thread.sleep(2000);
    } catch (Exception e) {}

    fs.checkpath();

    //System.out.println("Sleeping...");
    while (true) {
      //try{ Thread.sleep(5000); } catch (Exception e) {}
      Socket sk = sS.accept();
      new Thread(new fileServerListener(sk, fs.dP)).start();
    }
  }

  public fileServer(String zkHost, String myHost) throws IOException {
    this.myHost = myHost;
    
    //Parse the dictionary into partitions
    //...
    FileReader fr = new FileReader(dictionaryPath);
    BufferedReader textReader = new BufferedReader(fr);

    String word = "";
    String partition = "";
    int wordCount = 0;
    int partitionID = 0;

    while ((word = textReader.readLine()) != null) {
      //Reset if word limit reached
      if (wordCount == MAX_WORD_LIMIT) {
        dP[partitionID] = partition;
        //System.out.println(dP[partitionID]);
        partition = "";
        wordCount = 0;
        partitionID++;
      }

      if (wordCount == 0) {
        partition = word;
      }
      else {
        partition = partition+":"+word;
      }
      wordCount++;
    }
    dP[partitionID] = partition;
    //System.out.println(dP[partitionID]);

    //Connect to zooKeeper
    zkc = new ZkConnector();
    try {
      zkc.connect(zkHost);
    } catch (Exception e) {
      System.out.println("Zookeeper connect "+e.getMessage());
    }

    watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                  handleEvent(event);
                }
              };
  }

  private void checkpath() {
    Stat stat = zkc.exists(myPath, watcher);
    if (stat == null) {
      System.out.println("Creating " + myPath);
      Code ret = zkc.create(
                  myPath,   // Path of znode
                  myHost,   // Data of znode (host of this znode)
                  CreateMode.EPHEMERAL  // Znode type, set to EPHEMERAL
                );
      if (ret == Code.OK) System.out.println("Primary fileServer!");
    }
    else {
      System.out.println("Alive!");
    }
  }

  private void handleEvent(WatchedEvent event) {
    String path = event.getPath();
    EventType type = event.getType();
    if (path.equalsIgnoreCase(myPath)) {
      if (type == EventType.NodeDeleted) {
        System.out.println(myPath+" deleted! Let's go!");
        checkpath(); // Try to become the primary
      }
      if (type == EventType.NodeCreated) {
        System.out.println(myPath + " created!");
        try { Thread.sleep(5000); } catch (Exception e) {}
        checkpath();  // re-enable the watch
      }
    }
  }
}






