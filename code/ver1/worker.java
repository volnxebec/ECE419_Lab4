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

public class worker {

  public String wkGrpPath = "/workerGroup";
  public Watcher watcher;
  public ZkConnector zkc;

  public static void main(String[] args) {

    //Require 2 arguments
    //  arg1: zkServer:serverPort
    if (args.length !=1 ) {
      System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. fileServer zkServer:serverPort");
      return;
    }  

    worker wk = new worker(args[0]);

    while (true) {
      try{ Thread.sleep(5000); } catch (Exception e) {}
    }
                                

  }

  public worker(String zkHost) {

    zkc = new ZkConnector();
    try {
      zkc.connect(zkHost);
    } catch (Exception e) {
      System.out.println("Zookeeper connect "+e.getMessage());
    }

    //Create a workerGroup znode if doesn't exist...
    //ZooKeeper zk = zkc.getZooKeeper();
    //watcher = new Watcher();    
    createGroupZnode();
    

  }

  private void createGroupZnode() {
    Stat stat = zkc.exists(wkGrpPath, watcher);
    if (stat == null) {
      System.out.println("Creating " + wkGrpPath);
      Code ret = zkc.create(
                  wkGrpPath,
                  null,
                  CreateMode.PERSISTENT
                );
      if (ret == Code.OK) System.out.println("Created Worker Group Node!");
    }
    else {
      System.out.println("Worker Group Node Already Created!");
    }
  }


}





