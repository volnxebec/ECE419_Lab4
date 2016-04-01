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

public class JobTracker {

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

  // worker fault tolerance feature
  public List<String> failedWorkerList;

  // Defines
  public final int MAX_PARTITION = 27;

  // Primary-Backup fault tolerance variables
  public String myPath = "/jobTracker";
  public Watcher recoveryWatcher;
  public boolean primary = false;

  // Main function
  public static void main(String[] args) {

    //Require 1 argument
    //  arg1: zkServer:serverPort
    if (args.length != 1) {
      System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. jobTracker zkServer:serverPort");
      return;
    }

    JobTracker jt = new JobTracker(args[0]);

    //Other stuff code later
    jt.checkMyPath();
    //jt.start_run();

    //Don't die yet
    while (true) {
      try{ 
        Thread.sleep(5000); 
        //jt.initiateZnodes();
        if (jt.primary) jt.start_run();
        else System.out.println("waiting to be primary");
      } catch (Exception e) {}
    }
  }

  

  // Class functions
  public JobTracker(String zkHost) {
    
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

  //Main jobTracker Loop
  public void start_run() {
    //while (true) {
      try{ 
        Thread.sleep(5000); 
        //jt.initiateZnodes();
        System.out.println("sleeping");
      } catch (Exception e) {}

      // Check if new client exists
      // Client has 3 States
      //  1. OPEN
      //  2. INPROGRESS
      //  3. <result> (this means it is done)
      if (!clientList.isEmpty()) {
        System.out.println(clientList);
        for (String clientPath : clientList) {
          try {
            String fullClientPath = clientNode+"/"+clientPath;
            // Get clientPath data
            byte[] data = zk.getData(fullClientPath, false, null);
            String clientData = new String(data);
            System.out.println(clientPath+" data: "+clientData);

            // Check for clientPath state, if OPEN partition and change its state
            if (clientData.equals("OPEN")) {
              // Partition and Add to /jobs/open/<password>
              //  Also add znode for /jobs/inProgress/<password> and /jobs/done/<password>
              String fullJobOpenPath = openJobNode+"/"+clientPath;
              String fullJobInProgressPath = inProgressJobNode+"/"+clientPath;
              String fullJobDonePath = doneJobNode+"/"+clientPath;
              // TODO: probably need to change watcher for some of them...
              createPersistentZnodes(fullJobOpenPath, watcher);
              createPersistentZnodes(fullJobInProgressPath, watcher);
              createPersistentZnodes(fullJobDonePath, watcher);
              // Create sequential znodes for the password
              for (int i=0; i<MAX_PARTITION; i++) {
                createPersistentZnodes(fullJobOpenPath+"/"+i, watcher);
              }

              // Finally change state from OPEN -> INPROGRESS
              //  This is done at the end for recovery purposes...
              String inProgress = "INPROGRESS";
              data = inProgress.getBytes();
              Stat stat = zk.setData(fullClientPath, data, -1);
            } 


          } catch(KeeperException e) {
            System.out.println(e.code());
          } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
          }
        }
      }

      // Add Free Total workers to available workers znode
      //  total workers could have 2 different states
      //    1. FREE
      //    2. BUSY
      if (!totalWorkerList.isEmpty()) {
        for (String workerPath : totalWorkerList) {
          try {
            String fullWorkerPath = totalWorkerNode+"/"+workerPath;
            String fullFreeWorkerPath = availableWorkerNode+"/"+workerPath;
            // Get state of worker
            byte[] data = zk.getData(fullWorkerPath, false, null);
            String workerState = new String(data);
            // Add to free worker iff state==FREE
            if (workerState.equals("FREE")) {
              if (createPersistentZnodes(fullFreeWorkerPath, watcher)) {
                System.out.println("Adding "+workerPath+" to FREE worker List");
              }
            }
          } catch (KeeperException e) {
            System.out.println(e.code());
          } catch (Exception e) {
            System.out.println("Make node:" + e.getMessage());
          }
  
        }
      }

      // Check first for available workers
      //  TODO: NEED FAULT TOLERANCE CHECK HERE
      if (!availableWorkerList.isEmpty()) {
        //Check for any open jobs available
        if (!openJobList.isEmpty()) {
          try {
            for (String freeWorkerPath : availableWorkerList) {
              String fullFreeWorkerPath = availableWorkerNode+"/"+freeWorkerPath;
              String fullBusyWorkerPath = occupiedWorkerNode+"/"+freeWorkerPath;
              String fullTotalWorkerPath = totalWorkerNode+"/"+freeWorkerPath;
              
              byte[] data;
              Stat stat;
              //Now loop over the openJobs
              for (String openJobPath : openJobList) {
                String fullOpenJobPath = openJobNode+"/"+openJobPath;
                String fullInProgressJobPath = inProgressJobNode+"/"+openJobPath;
                //Find how many children this job path have...
                List<String> openJobChildrenList = createChildrenWatch(fullOpenJobPath, watcher);
                if (openJobChildrenList.size() > 0) {
                  // Pick the first one, and assign it to this worker
                  String assignJobPath = openJobChildrenList.get(0);
                  System.out.println("Assigning "+freeWorkerPath+" to "+openJobPath+"/"+assignJobPath);
                  String fullOpenAssignJobPath = fullOpenJobPath+"/"+assignJobPath;
                  String fullInProgressAssignJobPath = fullInProgressJobPath+"/"+assignJobPath;
                  // Move the worker from Available->Occupied
                  //  Add the name of the passphrase to the occupied worker node
                  String jobDescription = openJobPath+"/"+assignJobPath;
                  createPersistentZnodes(fullBusyWorkerPath, jobDescription, watcher);
                  // Move the assigned job from OPEN->INPROGRESS
                  createPersistentZnodes(fullInProgressAssignJobPath, freeWorkerPath, watcher);
                  // Change the status of the total worker znode
                  String busy = "BUSY";
                  data = busy.getBytes();
                  stat = zk.setData(fullTotalWorkerPath, data, -1);
                  // Delete other nodes
                  zk.delete(fullOpenAssignJobPath, -1);     
                  zk.delete(fullFreeWorkerPath, -1);
                  break;
                } else {
                  continue;
                }
              }
            }
          } catch (KeeperException e) {
            System.out.println(e.code());
          } catch(Exception e) {  
            System.out.println("Make node:" + e.getMessage());
          }
        }
      }

      // For worker failures, need to check for the inProgress jobs and see
      //  if their respective workers have failed or not.
      // If they have, put the job back into open job list
      if (!inProgressJobList.isEmpty()) {
        for (String inPgJobPath : inProgressJobList) {
          String fullInPgJobPath = inProgressJobNode+"/"+inPgJobPath;
          List<String> thisInPgJobList = createChildrenWatch(fullInPgJobPath, watcher);
          byte[] data;
          Stat stat;
          for (String thisInPgJobPath : thisInPgJobList) {
            try {
              String fullThisInPgJobPath = fullInPgJobPath+"/"+thisInPgJobPath;
              // Get the data -> which worker is currently working on this
              data = zk.getData(fullThisInPgJobPath, false, null);
              String workerName = new String(data);
              // Check if the current worker is still alive
              if (!totalWorkerList.contains(workerName)) {
                String thisJobPath = inPgJobPath+"/"+thisInPgJobPath;
                String fullThisOpenJobPath = openJobNode+"/"+thisJobPath;
                // The current worker is dead... need to put the job back into OPEN
                System.out.println(workerName+" is dead, "+thisJobPath+" OPEN again");
                createPersistentZnodes(fullThisOpenJobPath, watcher);
                // Delete the occupied worker Node...
                String fullOccupiedWorkerPath = occupiedWorkerNode+"/"+workerName; 
                stat = zkc.exists(fullOccupiedWorkerPath, watcher);
                if (stat != null) {
                  zk.delete(fullOccupiedWorkerPath, -1);
                }
                // To make sure, also check the available worker Nodes...
                String fullAvailableWorkerPath = availableWorkerNode+"/"+workerName;
                stat = zkc.exists(fullAvailableWorkerPath, watcher);
                if (stat != null) {
                  zk.delete(fullAvailableWorkerPath, -1);
                }
                // Delete this in progress node
                zk.delete(fullThisInPgJobPath, -1);
              }
            } catch (KeeperException e) {
              System.out.println(e.code());
            } catch (Exception e) {
              System.out.println("Make node:" + e.getMessage());
            }
          }
        }
      }

      // Check for any jobs that are done...
      //  The Done jobs can have 2 states
      //    1. NO_RESULT
      //    2. <result>
      if (!doneJobList.isEmpty()) {
        for (String doneJobPath : doneJobList) {
          try {
            String fullDoneJobPath = doneJobNode+"/"+doneJobPath;
            String fullOpenJobPath = openJobNode+"/"+doneJobPath;
            String fullInProgressJobPath = inProgressJobNode+"/"+doneJobPath;

            // Count how many children each password path in Done has...
            List<String> doneJobChildrenList = createChildrenWatch(fullDoneJobPath, watcher);

            // In case of failure recovery...
            if (!clientList.contains(doneJobPath)) {
              // Now delete all nodes...
              for (String jobResultPath : doneJobChildrenList) {
                String fullJobResultPath = fullDoneJobPath+"/"+jobResultPath;
                zk.delete(fullJobResultPath, -1);
              }
              zk.delete(fullDoneJobPath, -1);
              continue;
            }

            // For fault tolerance, check if the current done node still exists in clientList
            // If all of the jobs are done, let's find the final result
            byte[] data;
            String actualResult = "";
            Stat stat;
            String finalResult = "NO_RESULT";
            if (doneJobChildrenList.size() == MAX_PARTITION) {
              // Initialize the done job data in case of failure
              for (String jobResultPath : doneJobChildrenList) {
                String fullJobResultPath = fullDoneJobPath+"/"+jobResultPath;
                // Get the result string of this job
                data = zk.getData(fullJobResultPath, false, null);
                actualResult = new String(data);
                System.out.println(fullJobResultPath+" has result: "+actualResult);
                // Check if is correct result, if it is, updated final result
                if (!actualResult.equals("NO_RESULT")) {
                  finalResult = actualResult;  
                }
                //Now delete this node...
                //zk.delete(fullJobResultPath, -1);
              }
              //Update the client result
              String fullClientPath = clientNode+"/"+doneJobPath;
              data = finalResult.getBytes();
              stat = zkc.exists(fullClientPath, watcher);
              if (stat != null) {
                stat = zk.setData(fullClientPath, data, -1);
              }
            }
          } catch(KeeperException e) {
            System.out.println(e.code());
          } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
          }
        }
      }

      // Periodic openJob and inProgressJob cleanup...
      if (!openJobList.isEmpty()) {
        for (String openJobPath : openJobList) {
          try {
            String fullOpenJobPath = openJobNode+"/"+openJobPath;
            List<String> openJobChildrenList = createChildrenWatch(fullOpenJobPath, watcher); 
            if (!clientList.contains(openJobPath)) {
              // Now delete all nodes...
              for (String jobResultPath : openJobChildrenList) {
                String fullJobResultPath = fullOpenJobPath+"/"+jobResultPath;
                zk.delete(fullJobResultPath, -1);
              }
              zk.delete(fullOpenJobPath, -1);
            }
          } catch(KeeperException e) {
            System.out.println(e.code());
          } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
          }
        }
      }
      if (!inProgressJobList.isEmpty()) {
        for (String inPgJobPath : inProgressJobList) {
          try {
            String fullInPgJobPath = inProgressJobNode+"/"+inPgJobPath;
            List<String> inPgJobChildrenList = createChildrenWatch(fullInPgJobPath, watcher); 
            if (!clientList.contains(inPgJobPath) && inPgJobChildrenList.isEmpty()) {
              zk.delete(fullInPgJobPath, -1);
            }
          } catch(KeeperException e) {
            System.out.println(e.code());
          } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
          }
        }
      }

      // Periodic available worker list cleanup...
      if (!availableWorkerList.isEmpty()) {
        for (String freeWorkerPath : availableWorkerList) {
          try {
            String fullFreeWorkerPath = availableWorkerNode+"/"+freeWorkerPath;
            if (!totalWorkerList.contains(freeWorkerPath)) {
              zk.delete(fullFreeWorkerPath, -1);
            }
          } catch(KeeperException e) {
            System.out.println(e.code());
          } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
          }
        }
      }
    //}
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

  private boolean createPersistentZnodes(String path, Watcher w) {
    Stat stat = zkc.exists(path, w);
    boolean success = false;
    if (stat == null) {
      System.out.println("Creating " + path);
      Code ret = zkc.create(
              path,
              null,
              CreateMode.PERSISTENT
            );
      success = true;
    }
    return success;
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

  // Different Watcher event handling functions
  private void handleClientEvent(WatchedEvent event) {
    System.out.println("handleClientEvent");
    clientList = createChildrenWatch(clientNode, clientWatcher);
    System.out.println("clientList: "+clientList);
  }

  private void handleOpenJobEvent(WatchedEvent event) {
    System.out.println("handleOpenJobEvent");
    openJobList = createChildrenWatch(openJobNode, openJobWatcher);
    System.out.println("openJobList: "+openJobList);
  }

  private void handleInProgressJobEvent(WatchedEvent event) {
    System.out.println("handleInProgressJobEvent");
    inProgressJobList = createChildrenWatch(inProgressJobNode, inProgressJobWatcher);
    System.out.println("inProgressJobList: "+inProgressJobList);
  }

  private void handleDoneJobEvent(WatchedEvent event) {
    System.out.println("handleDoneJobEvent");
    doneJobList = createChildrenWatch(doneJobNode, doneJobWatcher);
    System.out.println("doneJobList: "+doneJobList);
  }

  private void handleAvailableWorkerEvent(WatchedEvent event) {
    System.out.println("handleAvailableWorkerEvent");
    availableWorkerList = createChildrenWatch(availableWorkerNode, availableWorkerWatcher);
    System.out.println("availableWorkerList: "+availableWorkerList);
  }

  private void handleOccupiedWorkerEvent(WatchedEvent event) {
    System.out.println("handleOccupiedWorkerEvent");
    occupiedWorkerList = createChildrenWatch(occupiedWorkerNode, occupiedWorkerWatcher);
    System.out.println("occupiedWorkerList: "+occupiedWorkerList);
  }

  private void handleTotalWorkerEvent(WatchedEvent event) {
    System.out.println("handleTotalWorkerEvent");
    //Before updating totalWorkerList, first check to see if it was a failure...
    //for (String workerPath : totalWorkerList) {
    //  String fullWorkerPath = totalWorkerNode+"/"+workerPath;
    //  Stat stat = zkc.exists(fullWorkerPath, watcher);
    //  // This worker failed
    //  if (stat == null) {
    //    failedWorkerList.add(workerPath);
    //    System.out.println(
    //  }
    //}
    totalWorkerList = createChildrenWatch(totalWorkerNode, totalWorkerWatcher);
    System.out.println("totalWorkerList: "+totalWorkerList);
  }

  private void handleRecoveryEvent(WatchedEvent event) {
    String path = event.getPath();
    EventType type = event.getType();
    if (path.equalsIgnoreCase(myPath)) {
      if (type == EventType.NodeDeleted) {
        System.out.println(myPath+" deleted! Let's go!");
        checkMyPath(); // Try to become the primary
      }
      if (type == EventType.NodeCreated) {
        System.out.println(myPath + " created!");
        try { Thread.sleep(5000); } catch (Exception e) {}
        checkMyPath();  // re-enable the watch
      }
    }
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

    recoveryWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        handleRecoveryEvent(event);
      }
    };
  }

  private void checkMyPath() {
    Stat stat = zkc.exists(myPath, recoveryWatcher);
    if (stat == null) {
      System.out.println("Creating " + myPath);
      Code ret = zkc.create(
                  myPath,
                  null,
                  CreateMode.EPHEMERAL  // Znode type, set to EPHEMERAL
                );
      if (ret == Code.OK) {
        System.out.println("Primary jobTracker!");
        this.primary = true;
        //this.start_run();
      }
    }
  }



}



















