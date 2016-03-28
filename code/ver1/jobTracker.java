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

public class jobTracker {

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
  public Watcher clientWatcher;
  public Watcher openJobWatcher;
  public Watcher inProgressJobWatcher;
  public Watcher doneJobWatcher;
  public Watcher availableWorkerWatcher;
  public Watcher occupiedWorkerWatcher;
  public Watcher totalWorkerWatcher;

  // ZooKeeper variables...
  public ZkConnector zkc;
  public ZooKeeper zk;

  // znode children lists
  public List<String> clientList;
  public List<String> openJobList;
  public List<String> inProgressJobList;
  public List<String> doneJobList;
  public List<String> availableWorkerList;
  public List<String> occupiedWorkerList;
  public List<String> totalWorkerList;

  // Main function
  public static void main(String[] args) {

    //Require 1 argument
    //  arg1: zkServer:serverPort
    if (args.length != 1) {
      System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. jobTracker zkServer:serverPort");
      return;
    }

    jobTracker jt = new jobTracker(args[0]);

    //Other stuff code later

    //Don't die yet
    while (true) {
      try{ 
        Thread.sleep(5000); 
        //jt.initiateZnodes();
        System.out.println("sleeping");
      } catch (Exception e) {}

    }
  }

  

  // Class functions
  public jobTracker(String zkHost) {
    
    //Connect to zooKeeper
    zkc = new ZkConnector();
    try {
      zkc.connect(zkHost);
    } catch (Exception e) {
      System.out.println("Zookeeper connect "+e.getMessage());
    }

    zk = zkc.getZooKeeper();
  
    initiateAllWatchers();
    initiateZnodes();
    initiateChildrenWatch();
              
  }

  // Initialize znode children watch...
  private void initiateChildrenWatch() {
    clientList = createChildrenWatch(clientNode, clientWatcher);     
    openJobList = createChildrenWatch(openJobNode, openJobWatcher);     
    inProgressJobList = createChildrenWatch(inProgressJobNode, inProgressJobWatcher);     
    doneJobList = createChildrenWatch(doneJobNode, doneJobWatcher);     
    availableWorkerList = createChildrenWatch(availableWorkerNode, availableWorkerWatcher);     
    occupiedWorkerList = createChildrenWatch(occupiedWorkerNode, occupiedWorkerWatcher);     
    totalWorkerList = createChildrenWatch(totalWorkerNode, totalWorkerWatcher);     
  }

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

  // Create the znodes if they don't exist yet
  private void initiateZnodes() {
     createPersistentZnodes(clientNode, watcher); 
     createPersistentZnodes(jobNode, watcher); 
     createPersistentZnodes(openJobNode, watcher); 
     createPersistentZnodes(inProgressJobNode, watcher); 
     createPersistentZnodes(doneJobNode, watcher); 
     createPersistentZnodes(workerGroupNode, watcher); 
     createPersistentZnodes(availableWorkerNode, watcher); 
     createPersistentZnodes(occupiedWorkerNode, watcher); 
     createPersistentZnodes(totalWorkerNode, watcher); 
  }

  private void createPersistentZnodes(String path, Watcher w) {
    Stat stat = zkc.exists(path, w);
    if (stat == null) {
      System.out.println("Creating " + path);
      Code ret = zkc.create(
              path,
              null,
              CreateMode.PERSISTENT
            );
    }
  }

  // Different Watcher event handling functions
  private void handleClientEvent(WatchedEvent event) {
    System.out.println("handleClientEvent");
    clientList = createChildrenWatch(clientNode, clientWatcher);
  }

  private void handleOpenJobEvent(WatchedEvent event) {
    System.out.println("handleOpenJobEvent");
    openJobList = createChildrenWatch(openJobNode, openJobWatcher);
  }

  private void handleInProgressJobEvent(WatchedEvent event) {
    System.out.println("handleInProgressJobEvent");
    inProgressJobList = createChildrenWatch(inProgressJobNode, inProgressJobWatcher);
  }

  private void handleDoneJobEvent(WatchedEvent event) {
    System.out.println("handleDoneJobEvent");
    doneJobList = createChildrenWatch(doneJobNode, doneJobWatcher);
  }

  private void handleAvailableWorkerEvent(WatchedEvent event) {
    System.out.println("handleAvailableWorkerEvent");
    availableWorkerList = createChildrenWatch(availableWorkerNode, availableWorkerWatcher);
  }

  private void handleOccupiedWorkerEvent(WatchedEvent event) {
    System.out.println("handleOccupiedWorkerEvent");
    occupiedWorkerList = createChildrenWatch(occupiedWorkerNode, occupiedWorkerWatcher);
  }

  private void handleTotalWorkerEvent(WatchedEvent event) {
    System.out.println("handleTotalWorkerEvent");
    totalWorkerList = createChildrenWatch(totalWorkerNode, totalWorkerWatcher);
  }

  private void initiateAllWatchers() {
    //Initiate all watchers
    clientWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        handleClientEvent(event);
      }
    };
                                   
    openJobWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        handleOpenJobEvent(event);
      }
    };
                                   
    inProgressJobWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        handleInProgressJobEvent(event);
      }
    };
                                   
    doneJobWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        handleDoneJobEvent(event);
      }
    };
                                   
    availableWorkerWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        handleAvailableWorkerEvent(event);
      }
    };
                                   
    occupiedWorkerWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        handleOccupiedWorkerEvent(event);
      }
    };
                                   
    totalWorkerWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        handleTotalWorkerEvent(event);
      }
    };
  }


}



















