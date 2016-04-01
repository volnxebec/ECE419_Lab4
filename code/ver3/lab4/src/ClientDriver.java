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
  public String fileServerNode = "/fileServer";
  public String jobTrackerNode = "/jobTracker";
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
    // Get the full client path
    String fullClientPath = clientNode+"/"+myPassHash;
    // Checking status
    if (statusCheck) {
      String result = "";
      // First check for if the client still exists
      Stat stat = zkc.exists(fullClientPath, watcher);
      if (stat == null) {
        result = "Failed: Job not found";
      }
      // Client still exists
      else {
        // Now get what the client data says
        byte[] data;
        String clientStatus = "";
        try {
          data = zk.getData(fullClientPath, false, null);
          clientStatus = new String(data);
        } catch (KeeperException e) {
          System.out.println(e.code());
          System.exit(1);
        } catch (Exception e) {
          System.out.println("Make node:" + e.getMessage());
          System.exit(1);
        }
        // Job still in progress...
        //  However if any of the other components are down, we cannot finish as well
        if (clientStatus.equals("INPROGRESS") || clientStatus.equals("OPEN")) {
          result = "In Progress";
          // First need to check if jobTracker or fileServer is down, and if there are any workers
          Stat jtStat = zkc.exists(jobTrackerNode, watcher);
          Stat fsStat = zkc.exists(fileServerNode, watcher);
          List<String> wkList = createChildrenWatch(totalWorkerNode, watcher);
          if (jtStat == null || fsStat == null || wkList.isEmpty()) {
            result = "Failed: Failed to complete job";
          }
        }
        // Job completed
        else {
          if (clientStatus.equals("NO_RESULT")) {
            result = "Failed: Password not found";
          } else {
            result = "Password found: "+clientStatus;
          }  
        }
      }
      System.out.println(result);
    }
    // Submitting a new job
    else {
      boolean clientCreated = createPersistentZnodes(fullClientPath, "OPEN", watcher);
      if (clientCreated) {
        System.out.println("SUCCESSFULLY CREATED CLIENT FOR PASSWORD: "+myPassHash);
      }
      else {
        System.out.println("FAILED TO CREATE CLIENT FOR PASSWORD: "+myPassHash);
      }
    }
  }

  // Helper functions
  private List<String> createChildrenWatch(String path, Watcher w) {
    List<String> children = null;
    try {
      children = zk.getChildren(path, w);
    } catch(KeeperException e) {
      System.out.println(e.code());
    } catch(Exception e) {
      System.out.println("Make node:" + e.getMessage());
    }
    return children;
  }

  private boolean createPersistentZnodes(String path, String data, Watcher w) {
    Stat stat = zkc.exists(path, w);
    boolean success = false;
    if (stat == null) {
      System.out.println("Creating " + path);
      Code ret = zkc.create(
                  path,
                  data,
                  CreateMode.PERSISTENT
                 );
      success = true;
    }
    return success;
  }
                                                                                                
}
























