package project3;


import java.io.*;
import java.net.*;

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

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import org.apache.thrift.TException;



public class CentralNode {

  public static CentralNodeServiceHandler handler;

   public static CentralNodeService.Processor processor;
   static int nodePort;

   public static int randInt(int min, int max) {


	   Random rand = new Random();
	   int randomNum = rand.nextInt((max - min) + 1) + min;

	   return randomNum;
	 }


 public static void StartsimpleServer(CentralNodeService.Processor<CentralNodeServiceHandler> processor, int port) {
  try {
   TServerTransport serverTransport = new TServerSocket(port);

   TServer server = new TThreadPoolServer(new
           TThreadPoolServer.Args(serverTransport).processor(processor));

   System.out.println("Starting CentralNode in Address: "+getHostAddress()+" and Port: "+nodePort);
   server.serve();
  } catch (Exception e) {
   e.printStackTrace();
  }
 }


 public static void monitorTimeout()
 {
	 for(;;)
	 {
		 try {
			 Thread.sleep(100);                 //1000 milliseconds is one second.
		 } catch(InterruptedException ex) {
			 Thread.currentThread().interrupt();
		 }
		 //System.out.println("Issuing pings to compute nodes....");

		 ArrayList<NodeName> ListOfNodes = CentralNodeServiceHandler.ListOfNodes;
		 for(int i=0;i<ListOfNodes.size();i++){
			 NodeName CurrentNode = ListOfNodes.get(i);

			 //System.out.println("Connecting to: " + CurrentNode.getIP() + ":" + CurrentNode.getPort()+":"+ CurrentNode.getID());


			 try{
				 TTransport NodeTransport;
				 NodeTransport = new TSocket(CurrentNode.getIP(), CurrentNode.getPort());
				 NodeTransport.open();
				 //NodeTransport.setTimeout(1);
				 TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
				 ComputeNodeService.Client nodeclient = new ComputeNodeService.Client(NodeProtocol);
				 if(nodeclient.ping(true) && NodeTransport.isOpen()){
					 //System.out.println("Ping message successfully replied back from "+ CurrentNode.getIP() + ":" + CurrentNode.getPort()+":"+ CurrentNode.getID());
				 }
				 else{
					 System.out.println("Ping message failed from "+ CurrentNode.getIP() + ":" + CurrentNode.getPort()+":"+ CurrentNode.getID());
					 System.out.println("Handling Fault");
					 //CentralNodeServiceHandler.HandleFault(CurrentNode);
				 }
				 NodeTransport.close();

			 }
			 catch(TException e){
				 //if(e.getCause() instanceof java.net.SocketTimeoutException){
					 System.out.println("Not able to connect: ");
					 System.out.println("Ping message failed from "+ CurrentNode.getIP() + ":" + CurrentNode.getPort()+":"+ CurrentNode.getID());
					 System.out.println("Handling Fault");
					 CentralNodeServiceHandler.HandleFault(CurrentNode);
				 //}
			 }
		 }
	 }
 }



 public static void runBackgroundUpdateService(int port)
 {
   try {


       Runnable simple = new Runnable() {
         public void run() {
           monitorTimeout();
         }
       };

       new Thread(simple).start();
       System.out.println("Starting Heartbeat service..." + port);
     } catch (Exception x) {
       x.printStackTrace();
     }
 }




 public static void main(String[] args) {
  //StartsimpleServer(new CentralNodeService.Processor<CentralNodeServiceHandler>(new CentralNodeServiceHandler()));


	 runBackgroundUpdateService(9092);

    try {
        handler = new CentralNodeServiceHandler();
        processor = new CentralNodeService.Processor(handler);
        if(args.length != 0)
        {
          //mode = Integer.parseInt(args[0]);
          System.out.println(args[0]);
          handler._CHUNK_SIZE_ = Integer.parseInt(args[0])*1024;
          handler._MERGE_COUNT_ = Integer.parseInt(args[1]);
          /*if(args[0].equals("coordinator")){
            isCoordinator = true;
            System.out.println("I am a coordinator");
            runBackgroundUpdateService(9092);
          }*/
        }
        Runnable simple = new Runnable() {
          public void run() {
        	nodePort = randInt(9000, 9090);
        	//Write to a file
        	try (PrintWriter writer = new PrintWriter("CentralNodeInfo.txt", "UTF-8")){
        		writer.println(getHostAddress());
        		writer.println(nodePort);
        	}catch(IOException e){

			 }

        	//
            StartsimpleServer(processor, nodePort);
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
		   String nodeName = addr.getHostAddress();
		   	return (nodeName);
		 } catch (UnknownHostException e) {
			 return null;
		 }
 }



}
