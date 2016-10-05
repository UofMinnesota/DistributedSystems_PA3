package project3;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.io.*;
import java.util.*;
import static java.lang.Math.*;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;

import project3.ComputeNodeServiceHandler.UpdateInfo;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.lang.String;
import java.util.Random;
import java.math.RoundingMode;
import java.text.DecimalFormat;

public class CentralNodeServiceHandler implements CentralNodeService.Iface {

//  public class  NodeInfo{
//    String address = "";
//    int port = 0;
//  }


  public static ArrayList<NodeName> ListOfNodes = new ArrayList<NodeName>();
  boolean isBusy = false;
  private static Random randomNum = new Random();
  private static int max_keys = 16;
  private int num_keys = max_keys;
  ArrayList<Integer> ListOfID = new ArrayList<Integer>();
  private static Map<String, FileStore> filestore = new HashMap<String, FileStore>();
  File InputDir;
  File OutputDir;
  File InterDir;
	static int isListLocked = 0;
	public static int _CHUNK_SIZE_ = 1024;
	private static int numFaults = 0;
	private static long executionTime=0;
	private static int numFileChunks=0;
	public static int _MERGE_COUNT_ = 2;




  //TODO redefine constructor later
  public CentralNodeServiceHandler()
  {
	  InputDir = new File("./InputDir");

	   if (!InputDir.exists()) {
           if (InputDir.mkdir()) {
               System.out.println("Directory is created!");
           } else {
               System.out.println("Failed to create directory!");
           }
       }

	   OutputDir = new File("./OutputDir");

	   if (!OutputDir.exists()) {
           if (OutputDir.mkdir()) {
               System.out.println("Directory is created!");
           } else {
               System.out.println("Failed to create directory!");
           }
       }

	   InterDir = new File("./InterDir");

	   if (!InterDir.exists()) {
           if (InterDir.mkdir()) {
               System.out.println("Directory is created!");
           } else {
               System.out.println("Failed to create directory!");
           }
       }
  }

 @Override
 public String Join(String IP, int Port) throws TException {



	 System.out.println("Node "+ IP+" : "+Port+" requests for joining Replica Network...");


    String NodeList = " ";

	   NodeName newNode = new NodeName(IP, Port);
	   //newNode.address = IP;
	   //newNode.port = Port;



	   boolean presentFlag=false;
	   for(int i=0;i<ListOfNodes.size();i++){
		   if(ListOfNodes.get(i).getIP().equals(IP) && ListOfNodes.get(i).getPort() == Port){
			   presentFlag=true;
			   break;
		   }
	   }

	   if(!presentFlag){
			 if(isListLocked == 1)
			 {
				 try{
				 Thread.sleep(10);
			 }catch(Exception e){}
			 }
			 isListLocked = 1;
			 ListOfNodes.add(newNode);
			 isListLocked = 0;
	   //ListOfNodes.add(newNode);
	   }


	  System.out.println("compute server "+ IP+" : "+Port+" joined  Replica Network...");

		  return "done";

 }

 @Override
 public String ClientSort(String FileName) throws TException {

	 String status = " ";
	 System.out.println("starting file split");
	 File inputFile = new File(InputDir, FileName);
	 int partCounter = 0;
	 boolean check = inputFile.exists();
	 System.out.println("Size of the current file is "+(inputFile.length()/_CHUNK_SIZE_));
	 if(!check){
		 System.out.println(FileName+" cannot be located in the "+InputDir+" directory. Please specify location relative to current directory.");
		 status = "fail";
	 }
	 else{
		 System.out.println(FileName+ " is present in the "+ InputDir+" directory. Preparing for sort...");
		 status = "pass";



		 //byte[] buffer = new byte[(int)(inputFile.length()/ListOfNodes.size())];
		 byte[] buffer = new byte[(int)(_CHUNK_SIZE_)];


		 try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(inputFile))) {
			 String name = inputFile.getName();

			 int tmp = 0;
			 while ((tmp = bis.read(buffer)) > 0) {
				 System.out.println("buffer points to "+ tmp);
				 File newFile = new File(InterDir, name + "."
						 + String.format("%03d", partCounter++));
				 try (FileOutputStream out = new FileOutputStream(newFile)) {
					 out.write(buffer, 0, tmp);
					 if(buffer[buffer.length-1] != 32 ){
						 byte[] tmp_buffer = new byte[1];
						 int eof=1;
						 while(tmp_buffer[0] != 32 && eof>0){
							 eof= bis.read(tmp_buffer);
							 out.write(tmp_buffer, 0, 1);
						 }
					 }
				 }
				 catch(IOException e){

				 }
				 RandomAccessFile rndfile = new RandomAccessFile(newFile, "r");
				 rndfile.seek(newFile.length()-1);
				 byte[] bytes = new byte[1];
				 rndfile.read(bytes);
				 rndfile.close();

				 //System.out.println("Last char read is "+bytes[0]);

			 }
		 }
		 catch(IOException e){

		 }


	 }
	 System.out.println("Ending file split");
	 System.out.println("Filename is "+ FileName+" and Part Count is "+ partCounter);
	 numFileChunks=partCounter;
	 FileStore fn = new FileStore(FileName,partCounter);

	 filestore.put(FileName, fn);

	 for(int i = 0; i < partCounter ; i++){
		 int nodeNext = i%ListOfNodes.size();
		 ListOfNodes.get(nodeNext).addSortJob(FileName + "."+ String.format("%03d", i));
		 //update NodeInfo here
		 //Call all sorts here
		 System.out.println("My next node is "+ ListOfNodes.get(nodeNext).getIP());
		 try {

			 TTransport NodeTransport;
			 NodeTransport = new TSocket(ListOfNodes.get(nodeNext).getIP(),ListOfNodes.get(nodeNext).getPort());
			 NodeTransport.open();

			 TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
			 ComputeNodeService.Client nodeclient = new ComputeNodeService.Client(NodeProtocol);
			 System.out.println("Sending out Sort Jobs for "+FileName + "."+ String.format("%03d", i)+" to the Compute..." );
			 nodeclient.Sort(FileName + "."+ String.format("%03d", i), 1,false);
			 NodeTransport.close();
			 filestore.get(FileName).setStatus("File "+FileName+" is sent for Sort to Compute Nodes...");
		 } catch (TException xx) {
			 xx.printStackTrace();
		 }
	 }

	 FileStore currentFile = filestore.get(FileName);
	 //int sleeptime = 1000;
	 while(currentFile.getMergeStatus() == false)
	 {
		 //System.out.println("Reading in progress...... sleep");
		 try{
			 Thread.sleep(100);
		 }catch(Exception e) {
			 e.printStackTrace();
		 }
	 }


	 return filestore.get(FileName).getStatus();
 }

 @Override
 public String Heartbeat(String NodeName, int Port, int Seq) throws TException {

	 return "DummyFile";

 }

 @Override
 public void returnSort(String nodeName, int nodePort, String Filename, String Time){
	 
	 executionTime=System.currentTimeMillis();
	 System.out.println(" Result returned for sort of File "+Filename+" from Node: "+nodeName+" port: "+ nodePort);
	 //remove file from node based tracker
	 for(int i =0; i < ListOfNodes.size();i++){
		 //if(nodeName.equals(ListOfNodes.get(i).getIP()) && nodePort == ListOfNodes.get(i).getPort()){
			 ListOfNodes.get(i).removeSortJob(Filename);
		 //}
	 }
	 //update completion status of the part in file based info
	 //String Fname = Filename.substring(0,Filename.length()-4);
	 //String partNum = Filename.substring(Filename.length()-3,Filename.length());
	 String[] fName = Filename.split("\\.");
	 String Fname = fName[0];
	 String partNum = fName[1];

	 System.out.println(" File name splitted in return sort is "+Fname+ " and part num is "+partNum);
	 int pnum = Integer.parseInt(partNum);

//	 FileStore fstr = filestore.remove(Fname);
//	 fstr.setSortStatus(pnum, true);
//	 filestore.put(Fname, fstr);

	 filestore.get(Fname).setSortStatus(pnum, true);


	 //Call for a file once all sort tasks complete
	 if(filestore.get(Fname).getSortStatus() && !filestore.get(Fname).getMergeStatus()){

		 int nodeNext = randomNum.nextInt(ListOfNodes.size());
		 ListOfNodes.get(nodeNext).addMergeJob(Fname);
		 filestore.get(Fname).setStatus("File is getting merged...");
		 //update NodeInfo here
		 //Call all sorts here
		 try {

		       TTransport NodeTransport;
		       NodeTransport = new TSocket(ListOfNodes.get(nodeNext).getIP(),ListOfNodes.get(nodeNext).getPort());
		       NodeTransport.open();

		       TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
		       ComputeNodeService.Client nodeclient = new ComputeNodeService.Client(NodeProtocol);
		       System.out.println("Sending out Merge JoB for "+Fname+" to the Compute..." );
		       nodeclient.Merge(Fname, 1,filestore.get(Fname).getNumParts(),_MERGE_COUNT_);
		       NodeTransport.close();
		       filestore.get(Fname).setStatus("File "+Fname+" is sent for merge to "+ListOfNodes.get(nodeNext).getIP()+":"+ListOfNodes.get(nodeNext).getPort()+"...");
		     } catch (TException xx) {
		       xx.printStackTrace();
		    }

	 }

	 //update statistics


 }

 @Override
 public void returnMerge(String nodeName, int nodeIP, String Filename, String Time){
	 //remove file from node based tracker
	 for(int i =0; i < ListOfNodes.size();i++){
		 //if(nodeName.equals(ListOfNodes.get(i).getIP()) && nodeIP == ListOfNodes.get(i).getPort()){
			 ListOfNodes.get(i).removeMergeJob(Filename);
		 //}
	 }

	 filestore.get(Filename).setMergeStatus(true);
	 filestore.get(Filename).setStatus("File "+Filename+" is completely sorted...");

	 
	 executionTime = System.currentTimeMillis()-executionTime;
	 System.out.println("Merge completed for file with Following Statistics: "+Filename);
	 System.out.println("ChunkSize of the File:		"+_CHUNK_SIZE_);
	 System.out.println("Number of chunks for the File:		"+numFileChunks);
	 System.out.println("Number of faults handled:		"+numFaults);
	 System.out.println("Execution Time of the system:		"+executionTime);
 }

 public static void HandleFault(NodeName FaultyNode){
	 numFaults++;

	 //Get a list of sort and merge tasks out from the faulty node
	 ArrayList<String> mergeList2 = FaultyNode.getMergeList();
	 ArrayList<String> mergeList = new ArrayList<String>();
	 for(int x = 0; x < mergeList2.size(); x++)
	 {
		 if(mergeList.contains(mergeList2.get(x))) continue;
		 mergeList.add(mergeList2.get(x));
	 }
	 ArrayList<String> sortList = FaultyNode.getSortList();
	 //Remove the node from the list of healthy nodes
	 for(int i =0;i<ListOfNodes.size();i++){
		 if(ListOfNodes.get(i).getIP().equals(FaultyNode.getIP()) && ListOfNodes.get(i).getPort() == FaultyNode.getPort()){
			 System.out.println("Faulty node is "+FaultyNode.getIP()+"and Port is "+FaultyNode.getPort());
			 if(isListLocked == 1)
			 {
				 try{
					 Thread.sleep(10);
				 }catch(Exception e){}
			 }
			 isListLocked = 1;
			 ListOfNodes.remove(i);
			 isListLocked = 0;
		 }
	 }


	 //Connect to any other node from the list and call merge or sort tasks from the list
	 if(ListOfNodes.size()>0){
		 for(int i =0;i<sortList.size();i++){
			 System.out.println("Size of Sort List is ..."+ ListOfNodes.size());
			 int nodeNext = randomNum.nextInt(ListOfNodes.size());;
			 ListOfNodes.get(nodeNext).addSortJob(sortList.get(i));
			 //if(sortList.get(i))
			 try {
				 TTransport NodeTransport;
				 NodeTransport = new TSocket(ListOfNodes.get(nodeNext).getIP(),ListOfNodes.get(nodeNext).getPort());
				 NodeTransport.open();

				 TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
				 ComputeNodeService.Client nodeclient = new ComputeNodeService.Client(NodeProtocol);
				 System.out.println("Sending out Sort Jobs for "+sortList.get(i)+" to the Compute Node through Fault tolerence..." );
				 //filestore.get(sortList.get(i)).setStatus("File is sorted through fault tolerence...");
				 nodeclient.Sort(sortList.get(i), 1,false);
				 NodeTransport.close();

			 } catch (TException xx) {
				 xx.printStackTrace();
			 }
		 }




		 for(int i =0;i<mergeList.size();i++){
			 int nodeNext = randomNum.nextInt(ListOfNodes.size());
			 ListOfNodes.get(nodeNext).addMergeJob(mergeList.get(i));
			 System.out.println("Merging file "+mergeList.get(i)+" iteration i= "+i+" size of mergelist "+mergeList.size());

			 try {

				 TTransport NodeTransport;
				 NodeTransport = new TSocket(ListOfNodes.get(nodeNext).getIP(),ListOfNodes.get(nodeNext).getPort());
				 NodeTransport.open();

				 TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
				 ComputeNodeService.Client nodeclient = new ComputeNodeService.Client(NodeProtocol);
				 System.out.println("Sending out Merge JoB for "+mergeList.get(i)+" to Compute Node through Fault tolerence..." );
				 filestore.get(mergeList.get(i)).setStatus("File is merged through fault tolerence...");
				 nodeclient.Merge(mergeList.get(i), 1,filestore.get(mergeList.get(i)).getNumParts(),_MERGE_COUNT_);
				 NodeTransport.close();
			 } catch (TException xx) {
				 xx.printStackTrace();
			 }
		 }
	 }
	 else{
		 System.out.println("All Nodes Failed...");
		 for ( String key : filestore.keySet() ) {
			 filestore.get(key).setStatus("All nodes are faulty. File Sorting failed..");
			 filestore.get(key).setMergeStatus(true);

		 }
	 }
 }



 public static String concatStringsWSep(String[] strings, String separator) {
     StringBuilder sb = new StringBuilder();
     String sep = "";
     for(String s: strings) {
       //System.out.println(s);
         sb.append(sep).append(s);
         sep = separator;
     }
     return sb.toString();
 }


 public String GetNodeList() {

	 String NodeList = " ";

  for(int x = 0; x < ListOfNodes.size(); x++)
	  {
		  NodeList += ListOfNodes.get(x).getIP() + ":" + String.valueOf(ListOfNodes.get(x).getPort()) +  ",";
	  }

  return NodeList;


 }



}
