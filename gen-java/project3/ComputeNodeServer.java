package project3;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import java.util.Random;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;


//Node Class

public class ComputeNodeServer {
  public static int randInt(int min, int max) {


  Random rand = new Random();
  int randomNum = rand.nextInt((max - min) + 1) + min;

  return randomNum;
}

  public ComputeNodeServer(){
	  getHostAddress();

  }
static boolean USE_LOCAL = false;
static boolean isCoordinator = false;
static int nodePort;
static String nodeName;
static String result;
public static ComputeNodeServiceHandler handler;
public static ComputeNodeService.Processor processor;
public static int lastUpdatedReplica = 0;
//static ArrayList<NodeName> ListOfNodes; // For coordinator
//static



 public static void StartsimpleServer(ComputeNodeService.Processor<ComputeNodeServiceHandler> processor) {
  try {
	  //nodeName = new String(getHostAddress());

   TServerTransport serverTransport = new TServerSocket(nodePort);
   //TServer server = new TSimpleServer(
   //  new Args(serverTransport).processor(processor));
   TServer server = new TThreadPoolServer(new
           TThreadPoolServer.Args(serverTransport).processor(processor));

   System.out.println("Establishing connection with the CentralNode...");

   TTransport CentralNodeTransport;
   String CentralNodeAddr = " ";
   int CentralPort =9090;
   if(USE_LOCAL){
	   CentralNodeAddr = "localhost";
   }
   else{
	   try(Scanner scan = new Scanner(new File("CentralNodeInfo.txt"))){
		   while(scan.hasNextLine()){
			   CentralNodeAddr=scan.nextLine();
			   CentralPort = Integer.parseInt(scan.nextLine());
		   }
	   }
	   catch(FileNotFoundException e ){

	   }
   }
   //String CentralNodeAddr = "csel-x30-11";
   //if(USE_LOCAL) CentralNodeAddr = "localhost";
   CentralNodeTransport = new TSocket(CentralNodeAddr, CentralPort); // csel-x29-10
   CentralNodeTransport.open();

   TProtocol CentralNodeProtocol = new TBinaryProtocol(CentralNodeTransport);
   CentralNodeService.Client CentralNodeclient = new CentralNodeService.Client(CentralNodeProtocol);

  System.out.println("Requesting CentralNode for joining Network through Join Call...");

  //nodeName = getHostAddress();
  System.out.println("My name is"+nodeName+"my port is "+nodePort);


  //ArrayList<NodeName> ListOfNodes = new ArrayList<NodeName>();
  //NodeName myName;
  String result;


  //send DHTList string to the nodeservicehandler
  //myName = ComputeNodeServiceHandler.getMyName();

  result = CentralNodeclient.Join(getHostAddress(),nodePort);

  handler.setJoinResult(result);
  System.out.println("---Result is------"+result);


    System.out.println("Joining Replica network...");


	   CentralNodeTransport.close();

	   System.out.println("Successfully joined replica network...");
	   System.out.println("Starting simple ComputeNodeServer...");
	   server.serve();

  } catch (Exception e) {
   e.printStackTrace();
  }
 }

 public static void updateReplicas()
 {
	 for(;;)
	 {
		 try {
			 Thread.sleep(10000);                 //1000 milliseconds is one second.
		 } catch(InterruptedException ex) {
			 Thread.currentThread().interrupt();
		 }
		 System.out.println("Issuing update to replica....");

		 ArrayList<NodeName> ListOfNodes = ComputeNodeServiceHandler.getNodes();
		 int NodeID = lastUpdatedReplica++;//randInt(0,ListOfNodes.size()); //random for testing purposes, should increment a var
		 if(lastUpdatedReplica  >= ListOfNodes.size())
		 {
			 lastUpdatedReplica = 0;
		 }
		 NodeName CurrentNode = ListOfNodes.get(NodeID);
		 Map<String,ComputeNodeServiceHandler.UpdateInfo> list = ComputeNodeServiceHandler.getfileinfo();



		 try{
			 TTransport NodeTransport;
			 System.out.println("Connecting to: " + CurrentNode.getIP() + ":" + CurrentNode.getPort()+":"+ CurrentNode.getID());
			 NodeTransport = new TSocket(CurrentNode.getIP(), CurrentNode.getPort());
			 NodeTransport.open();

			 TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
			 ComputeNodeService.Client nodeclient = new ComputeNodeService.Client(NodeProtocol);
			 for (String name: list.keySet()){
				 System.out.println("Writing File "+ name+ "Version: "+list.get(name).getVersion()+ " to node "+CurrentNode.getIP() + ":" + CurrentNode.getPort());
				 //nodeclient.serverWrite(name, list.get(name).getContent(), list.get(name).getVersion());
			 }
		 }
		 catch(Exception e){
			 System.out.println("Not able to connect: ");
		 }
	 }
 }

public static void runBackgroundUpdateService(int port)
{
  try {


      Runnable simple = new Runnable() {
        public void run() {
          updateReplicas();
        }
      };

      new Thread(simple).start();
      System.out.println("Starting background service..." + port);
    } catch (Exception x) {
      x.printStackTrace();
    }
}


 public static void main(String[] args) {
   //int mode = -1;

  //StartsimpleServer(new ComputeNodeService.Processor<ComputeNodeServiceHandler>(new ComputeNodeServiceHandler(isCoordinator,getHostAddress(),nodePort, result)));
  try {
      handler = new ComputeNodeServiceHandler(isCoordinator,getHostAddress(),nodePort);

      processor = new ComputeNodeService.Processor(handler);
      nodePort = randInt(9000, 9080);
      if(args.length != 0)
      {
        //mode = Integer.parseInt(args[0]);
        System.out.println(args[0]);
        handler._FAIL_PROB_ = Integer.parseInt(args[0]);
        /*if(args[0].equals("coordinator")){
          isCoordinator = true;
          System.out.println("I am a coordinator");
          runBackgroundUpdateService(9092);
        }*/
      }
      //System.out.println("My name is"+nodeName);

      Runnable simple = new Runnable() {
        public void run() {
          StartsimpleServer(processor);
        }
      };

      new Thread(simple).start();
    } catch (Exception x) {
      x.printStackTrace();
    }


 }

 private static String getHostAddress(){
	 try {
		   InetAddress addr = InetAddress.getLocalHost();
		   nodeName = addr.getHostAddress();
		   	return (nodeName);
		 } catch (UnknownHostException e) {
			 return null;
		 }
 }



}
