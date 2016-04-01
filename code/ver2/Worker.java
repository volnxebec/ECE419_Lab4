import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.net.Socket;

import java.io.IOException;

import java.util.*; 
import java.net.*;
import java.io.*;
import java.util.List;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.PrintWriter;

public class Worker {

  // Znode path of all nodes...
  public String fileServerNode = "/fileServer";
  public String jobTrackerNode = "/jobTracker";
  public String workerGroupNode = "/workerGroup";
  public String availableWorkerNode = workerGroupNode+"/available";
  public String occupiedWorkerNode = workerGroupNode+"/occupied";
  public String totalWorkerNode = workerGroupNode+"/total";
  public String clientNode = "/client";
  public String jobNode = "/jobs";
  public String openJobNode = jobNode+"/open";
  public String inProgressJobNode = jobNode+"/inProgress";
  public String doneJobNode = jobNode+"/done";

  // Watchers for znode types that need it
  public Watcher watcher;
  public Watcher myWorkerWatcher;

  // Znode children list
  public List<String> totalWorkerList;

  // ZooKeeper variables
  public ZooKeeper zk;
  public ZkConnector zkc;

  // Other variables
  public String myName = "worker";
  public String myStatus = "FREE";
  public String myWorkerPath = "";

  // FileServer connection variables
  public Socket fileServerSocket;
  public ObjectOutputStream toServer;
  public ObjectInputStream fromServer;

  // Hashing class
  public MD5Test md5Hasher;

  public static void main(String[] args) {
    //Require 2 arguments
    //  arg1: zkServer:serverPort
    if (args.length !=1 ) {
      System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. worker zkServer:serverPort");
      return;
    }  

    Worker wk = new Worker(args[0]);
    //String myName = wk.myName;
    wk.start_run();


    //while (true) {
    //  try{ 
    //    Thread.sleep(5000); 
    //    System.out.println(myName+": "+"Sleeping");
    //  } catch (Exception e) {}
    //}
  }

  public Worker(String zkHost) {

    zkc = new ZkConnector();
    try {
      zkc.connect(zkHost);
    } catch (Exception e) {
      System.out.println(myName+": "+"Zookeeper connect "+e.getMessage());
    }

    zk = zkc.getZooKeeper();

    initiateAllWatchers();
    initiateZnodes();
    initiateChildrenWatch();

    initiateMyWorker();

    md5Hasher = new MD5Test();
  }

  // Main worker loop
  public void start_run() {

    while (true) {
      try{ 
        Thread.sleep(5000); 
        System.out.println(myName+": "+"Sleeping");
      } catch (Exception e) {}

      // Check for my STATUS, do something if we are now in BUSY STATE
      //  Change back to FREE after this is done
      if (myStatus.equals("BUSY")) {
        System.out.println(myName+": "+"My Turn to Work");
        byte[] data;
        Stat stat;
        String jobPath = "";
        // Go find out which job to hash
        try {
          String fullBusyWorkerPath = occupiedWorkerNode+"/"+myName; 
          data = zk.getData(fullBusyWorkerPath, false, null);
          jobPath = new String(data);
          // Get hash result
          String hashResult = getPasswordHash(jobPath);
          // Update results to zookeeper by creating /jobs/done/jobPath
          //  and deleting /jobs/inProgress/jobPath
          String fullDoneJobPath = doneJobNode+"/"+jobPath;
          String fullInProgressJobPath = inProgressJobNode+"/"+jobPath;
          // First create done node and set result as its data
          createPersistentZnodes(fullDoneJobPath, hashResult, watcher);
          // Before deleting /jobs/inProgress/jobPath, 
          //  we need to delete this worker from /workerGroup/occupied first
          zk.delete(fullBusyWorkerPath, -1);
          zk.delete(fullInProgressJobPath, -1);
          // Now change my own status from FREE to BUSY
          String fullTotalWorkerPath = totalWorkerNode+"/"+myName;
          myStatus = "FREE";
          data = myStatus.getBytes();
          stat = zk.setData(fullTotalWorkerPath, data, -1);
          try{ 
            Thread.sleep(2000); 
            System.out.println(myName+": "+"Taking a Break");
          } catch (Exception e) {}
        } catch(KeeperException e) {
          System.out.println(e.code());
        } catch(Exception e) {
          System.out.println(myName+": "+"Make node:" + e.getMessage());
        }
      }
     
    }
  }

  // Password hash and partition function
  private String getPasswordHash(String jobPath) {
    String result = "NO_RESULT";
    String[] jobArray = jobPath.split("/");
    String password = jobArray[0];
    int partition = Integer.parseInt(jobArray[1]);

    // Hashing functions
    System.out.println(myName+": "+"Hashing... started");

    String[] dictionary = getDictionary(partition);

    for (String word : dictionary) {
      String testHash = md5Hasher.getHash(word);
      if (testHash.equals(password)) {
        result = word;
        break;
      }
    }

    System.out.println(myName+": "+"Hashing... finished");

    return result;
  }

  private String[] getDictionary(int p) {

    String fullDict = "";

    // Check to see if it exists first, if not wait for it to come back
    while (true) {
      Stat stat = zkc.exists(fileServerNode, watcher);
      if (stat == null) {
        // Check again...
        try{ 
          Thread.sleep(10000); 
          System.out.println(myName+": "+"waiting for fileServer to come back online 1");
        } catch (Exception ex) {}
        continue;
      }
      // If it does exist, get its port info
      byte[] data;
      String host = "";
      try {
        data = zk.getData(fileServerNode, false, null);
        host = new String(data);
      } catch(KeeperException e) {
        // If for some reason, reading the node failed, try again...
        System.out.println(e.code());
        try{ 
          Thread.sleep(10000); 
          System.out.println(myName+": "+"waiting for fileServer to come back online 2");
        } catch (Exception ex) {}
        continue;
      } catch(Exception e) {
        System.out.println(myName+": "+"Make node:" + e.getMessage());
      }
      // Parse the port info
      String[] hostInfo = host.split(":");
      String hostName = hostInfo[0];
      int hostPort = Integer.parseInt(hostInfo[1]);
      // Now try the socket connection...
      try {
        fileServerSocket = new Socket(hostName, hostPort);
        toServer = new ObjectOutputStream(fileServerSocket.getOutputStream());
        fromServer = new ObjectInputStream(fileServerSocket.getInputStream());
        // Sending partition ID to fileServer
        toServer.writeObject(p);
        fullDict = (String) fromServer.readObject();
        fileServerSocket.close();
        break;
      } catch(IOException e) {
        try{ 
          Thread.sleep(10000); 
          System.out.println (myName+": "+"can't connect to host right now, try again");
        } catch (Exception ex) {}
        continue;
      } catch(Exception e) {
        System.out.println(myName+": "+"Make node:" + e.getMessage());
      }
    }
    
    String[] dictArray = fullDict.split(":");
    System.out.println(fullDict);
    return dictArray;

  }

  // Initiate myself into the /workerGroup/total/ znode
  private void initiateMyWorker() {
    try {
      // Get the current version number by writing data to total worker node
      //  This will become this worker's ID
      Stat stat = zk.setData(totalWorkerNode, null, -1);
      myName = myName+"_"+stat.getVersion();
      myWorkerPath = totalWorkerNode+"/"+myName;
      Code ret = zkc.create(
                  myWorkerPath,
                  myStatus,
                  CreateMode.EPHEMERAL  // Znode type, set to EPHEMERAL
                 );
      if (ret == Code.OK) { 
        System.out.println("Creating " + myName+" in "+totalWorkerNode); 
      }
      // Now create a data watch for current worker
      byte[] data = zk.getData(myWorkerPath, myWorkerWatcher, null);
      myStatus = new String(data);
    } catch(KeeperException e) {
      System.out.println(e.code());
    } catch(Exception e) {
      System.out.println(myName+": "+"Make node:" + e.getMessage());
    }
  }

  // Watcher initialization
  private void initiateAllWatchers() {
    // Initiate children watcher for the /workerGroup/total znode
    myWorkerWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        handleMyWorkerEvent(event);
      }
    };
  }

  // Znodes initialization
  private void initiateZnodes() {
    createPersistentZnodes(clientNode, null, watcher); 
    createPersistentZnodes(jobNode, null, watcher); 
    createPersistentZnodes(openJobNode, null, watcher); 
    createPersistentZnodes(inProgressJobNode, null, watcher); 
    createPersistentZnodes(doneJobNode, null, watcher); 
    createPersistentZnodes(workerGroupNode, null, watcher); 
    createPersistentZnodes(availableWorkerNode, null, watcher); 
    createPersistentZnodes(occupiedWorkerNode, null, watcher); 
    createPersistentZnodes(totalWorkerNode, null, watcher);
  }

  // Children Watcher initialization
  private void initiateChildrenWatch() {
    //totalWorkerList = createChildrenWatch(totalWorkerNode, myWorkerWatcher);     
  }

  // Helper functions
  private List<String> createChildrenWatch(String path, Watcher w) {
    List<String> children = null;
    try {
      children = zk.getChildren(path, w);
    } catch(KeeperException e) {
      System.out.println(myName+": "+e.code());
    } catch(Exception e) {
      System.out.println(myName+": "+"Make node @ "+path+" :" + e.getMessage());
    }
    return children;
  }

  private void handleMyWorkerEvent(WatchedEvent event) {
    System.out.println(myName+": "+"handleMyWorkerEvent");
    try {
      byte[] data = zk.getData(myWorkerPath, myWorkerWatcher, null);
      myStatus = new String(data);
      System.out.println(myName+": "+"Status changed to: "+myStatus);
    } catch(KeeperException e) {
      System.out.println(e.code());
    } catch(Exception e) {
      System.out.println(myName+": "+"Make node:" + e.getMessage());
    }
    //totalWorkerList = createChildrenWatch(totalWorkerNode, myWorkerWatcher);
    //System.out.println(myName+": "+"totalWorkerList: "+totalWorkerList);
  }

  private boolean createPersistentZnodes(String path, String data, Watcher w) {
    Stat stat = zkc.exists(path, w);
    boolean success = false;
    if (stat == null) {
      System.out.println(myName+": "+"Creating " + path);
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





