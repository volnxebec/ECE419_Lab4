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

import java.util.*; 
import java.net.*;
import java.io.*;
import java.util.List;

public class ClientDriver { 

  // Znode path of interest for client
  public String clientNode = "/client";
  public String workerGroupNode = "/workerGroup";
  public String availableWorkerNode = workerGroupNode+"/available";
  public String occupiedWorkerNode = workerGroupNode+"/occupied";
  public String totalWorkerNode = workerGroupNode+"/total";

  // Watchers for znodes
  public Watcher watcher;

  // ZooKeeper variables...
  public ZkConnector zkc;
  public ZooKeeper zk;

  // Password variables
  public String myPassHash = "";
  public boolean statusCheck = false;

  // Main function
  public static void main(String[] args) {

    //Require 1 argument
    //  arg1: zkServer:serverPort 
    if (args.length != 3) {
      System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. ClientDriver zkServer:serverPort job/status password_hash");
      return;
    }

    ClientDriver cd = new ClientDriver(args);

    cd.start_run();
  }

  public ClientDriver(String[] args) {
    String zkHost = args[0];
    String option = args[1];
    myPassHash = args[2];

    // Connect to zooKeeper
    zkc = new ZkConnector();
    try {
      zkc.connect(zkHost);
    } catch (Exception e) {
      System.out.println("Zookeeper connect "+e.getMessage());
    }

    zk = zkc.getZooKeeper();

    // Figure out if it's a new job submission or status check
    if (option.equals("job")) {
      statusCheck = false;
    }
    else if (option.equals("status")) {
      statusCheck = true;
    }
    else {
      System.out.println("Bad job/status argument: "+option);
      System.exit(0);
    }
 
  }

  // Main client loop
  public void start_run() {
    System.out.println("Hello World, starting clientDriver");
  }


}
























