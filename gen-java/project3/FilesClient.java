package project3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

public class FilesClient {
  static boolean USE_LOCAL = false;

  public static class  NodeInfo implements Comparable<NodeInfo>{
    String address = "";
    int port = 0;
    int hash = 0;


    public int compareTo(NodeInfo N) {
        return Integer.compare(hash, N.hash);
    }


  }

  private static ArrayList<NodeName> ListOfNodes = new ArrayList<NodeName>();

  public static NodeName strToNodeName(String input)
  {
    String data[] = input.split(":");

    NodeName newNo = new NodeName(data[0].trim(),Integer.parseInt(data[1]),0);

    return newNo;
  }

  public static ArrayList<NodeName> strToNodeNameArray(String input)
  {
    ArrayList<NodeName> arrN = new ArrayList<NodeName>();
    String data[] = input.split("\\,");
    for(int c = 0; c < data.length; c++)
    {
      arrN.add(strToNodeName(data[c]));
    }
    return arrN;
  }

 public static void main(String[] args) {

   int mode = 0;
   String FileName="";


   if(args.length != 0)
   {
     FileName = args[0];
   }
   else{
	   System.out.println("Files Client has to be run with filename:");
	   System.out.println("Usage: ");
	   System.out.println("<client> <filename>");
   }

	   sortFile(FileName);

 }


 // Method for getting host address
 private static String getHostAddress(){
	 try {
		   InetAddress addr = InetAddress.getLocalHost();
		   	return (addr.getHostAddress());
		 } catch (UnknownHostException e) {
			 return null;
		 }
 }

//RMI Method for writing files

 public static void sortFile(String FileName){

	 try {

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

			 }catch(FileNotFoundException e){

			 }
		 }
		 CentralNodeTransport = new TSocket(CentralNodeAddr, CentralPort); // csel-x29-10
		 CentralNodeTransport.open();



		 TProtocol CentralNodeProtocol = new TBinaryProtocol(CentralNodeTransport);
		 CentralNodeService.Client CentralNodeClient = new CentralNodeService.Client(CentralNodeProtocol);
		 String Status = CentralNodeClient.ClientSort(FileName);
		 CentralNodeTransport.close();
		 System.out.println(Status);


	 }
	 catch (TException x) {
		 x.printStackTrace();
	 }

 }



public static int randInt(int min, int max) {


	  Random rand = new Random();
	  int randomNum = rand.nextInt((max - min)) + min;

	  return randomNum;
	}
}
