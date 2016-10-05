package project3;

import org.apache.thrift.TException;
import java.security.*;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Random;
import java.util.Collections;
import java.util.Comparator;
import java.util.Arrays;
import javax.print.DocFlavor.STRING;
import java.io.*;
import java.net.*;
import java.util.Scanner;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.TException;
import java.util.PriorityQueue;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ComputeNodeServiceHandler implements ComputeNodeService.Iface {

  public static class  NodeInfo implements Comparable<NodeInfo>{
    String address = "";
    int port = 0;
    int hash = 0;


    public int compareTo(NodeInfo N) {
        return Integer.compare(hash, N.hash);
    }


  }

  public class  UpdateInfo{

    private NodeName node;
    private Boolean isWrite;
    private String filename;
    private String content;
    private int version;

    UpdateInfo(NodeName n, String fn, int V, String c){
    	node=n;
    	filename=fn;
    	version=V;
    	content =c;
    }

    UpdateInfo(){

    }

    String getFilename(){
    	return filename;
    }

    String getNodename(){
    	return node.getIP();
    }

    String getContent(){
    	return content;
    }

    int getVersion(){
    	return version;
    }

    int nodePort(){
    	return node.getPort();
    }



  }

  File InputDir;
  File OutputDir;
  File InterDir;

  private static ArrayList<NodeName> ListOfNodes = new ArrayList<NodeName>();
  private static Map<String, FileStore> filestore = new HashMap<String, FileStore>();

  private static Map<String,UpdateInfo> fileinfo = new HashMap<String, UpdateInfo>();
  public static int _FAIL_PROB_ = 0;
  private static String DHTList;
  //private static int maxNumNodes = 16;
  private static boolean isCoordinator = false;
  private static boolean isRunningBg = false;
  private static NodeName CoordinatorName;
  private static NodeName myName;
  private static int readCount = 0;
  private static int writeCount = 0;
  private static boolean writeSignal=false;
  private static String joinResult;
  private static int threads [] = new int[10000];
  public static int exte0x3255 = 0;
  private static int Nr, Nw, N;
  public static BlockingQueue executeQueue = new ArrayBlockingQueue(1024);
  private Random randomGenerator = new Random();
  //private static int numjobs=0;
  private  int numSortJobs=0;
  private  int numMergeJobs=0;
  private  long SortTime=0;
  private  long MergeTime=0;
  //private static long TotalTime=0;

  public static ArrayList<NodeName> getListOfNodes(){

	  return ListOfNodes;
  }

  public void setJoinResult(String T){
	  joinResult = T;

  }


  public ComputeNodeServiceHandler(boolean isC, String name, int port)
  {
    //if(max == -1) return;
    //maxNumNodes = max;
	  //new File(InterDir, file.getName())
	  numSortJobs=0;
	  numMergeJobs=0;
	  SortTime=0;
	  MergeTime=0;
	  InputDir = new File("InputDir");
	  OutputDir = new File("OutputDir");
	  InterDir = new File("InterDir");
	  myName = new NodeName(name,port,0);
	  isCoordinator = isC;
	  if(exte0x3255 == 1) return;
    for(int x = 0; x<threads.length; x++)
    {
      threads[x] = 0;
    }
    exte0x3255 = 1;
  }

  public static Map<String,UpdateInfo> getfileinfo()
  {
	  return fileinfo;
  }

  public Boolean areThreadsRunning()
  {
	  if(exte0x3255 == 0) return true;
    for(int x = 0; x<threads.length; x++)
    {
      if(threads[x] == 1) return true;
    }
    return false;
  }

  public static NodeName getMyName(){
	  return myName;
  }

  public static ArrayList<NodeName> getNodes()
  {
    return ListOfNodes;
  }

  public static int getNumberOfFiles()
  {
    return filestore.size();
  }

  public static ArrayList<FileStore> getFileNames()
  {
    ArrayList<FileStore> filestoreArr = new ArrayList<FileStore>();
    for (String name: filestore.keySet()){

            String key =name.toString();
            FileStore value = filestore.get(name);
            //System.out.println(key);
            filestoreArr.add(value);



          }
    return filestoreArr;
  }



  public static NodeName strToNodeName(String input)
  {
    System.out.println("Input is "+input);
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

  public String MD5(String md5) {
     try {
          java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
          byte[] array = md.digest(md5.getBytes());
          StringBuffer sb = new StringBuffer();
          for (int i = 0; i < array.length; ++i) {
            sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1,3));
         }
          return sb.toString();
      } catch (java.security.NoSuchAlgorithmException e) {

      return null;}
  }










 @Override
 public int getVersionNumber(String Filename) throws TException {
	 //if(filestore.containsKey(Filename)) return (filestore.get(Filename).getVersion());
	 return -1;
 }


@Override
public void Merge(String FileName, int Priority, int Parts, int Merges){
  //System.out.println("Sort received for file "+ FileName);
    class MergeThread implements Runnable{
  	  String FileName;
  	  int Priority;
      int Parts;
      int sizemerg = 2;
  	 public MergeThread(String fn, int pri, int parts, int merges){
  		 FileName = fn;
       Parts = parts;
  		 Priority = pri;
       sizemerg = merges;
  	 }
      public void run() {
long time = System.currentTimeMillis();
	System.out.println("Merge received for file "+ FileName);
	ArrayList<String> results = new ArrayList<String>();

	String everything = "";
  String llocal = "";
	File[] files = (InterDir).listFiles();
	//If this pathname does not denote a directory, then listFiles() returns null.
  int xiek = 0;
	for (File file : files) {
		if (file.isFile()) {
			if(file.getName().contains(FileName+"."))
			{


				String patho = new File(InterDir, file.getName()).getAbsolutePath();
				String pathoII = new File(InterDir, file.getName()).getAbsolutePath();
				try(BufferedReader br = new BufferedReader(new FileReader(patho))) {
					StringBuilder sb = new StringBuilder();
					String line = br.readLine();

					while (line != null) {
						sb.append(line);
						sb.append(System.lineSeparator());
						line = br.readLine();
					}
					everything += sb.toString() + " ";
          llocal += sb.toString() + " ";

				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
        xiek++;
        if(xiek % sizemerg == 0 || xiek == Parts)
        {
          System.out.println("Merge received for file and complete "+ FileName);
        	String patho3 = new File(InterDir,FileName+"_merged"+xiek).getAbsolutePath();
        	try (Writer writer = new BufferedWriter(new OutputStreamWriter(
        			new FileOutputStream(patho3), "utf-8"))) {

        		writer.write(llocal);
            llocal = "";
        	}
        	catch(Exception e){

        	}
          Sort(FileName+"_merged"+xiek,Priority, true);
        }
			}
		}
	}
	System.out.println("Merge received for file and complete "+ FileName);
	String patho = new File(InterDir,FileName+"_merged").getAbsolutePath();
	try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			new FileOutputStream(patho), "utf-8"))) {
		writer.write(everything);
	}
	catch(Exception e){

	}
	Sort(FileName+"_merged",Priority, true);
	//return everything;

	TTransport CentralNodeTransport;
	String CentralNodeAddr = "localhost";
	int CentralPort =9090;

	try(Scanner scan = new Scanner(new File("CentralNodeInfo.txt"))){
		while(scan.hasNextLine()){
			CentralNodeAddr=scan.nextLine();
			CentralPort = Integer.parseInt(scan.nextLine());
		}
	}
	catch(FileNotFoundException e ){

	}


	try{
		CentralNodeTransport = new TSocket(CentralNodeAddr, CentralPort); // csel-x29-10
		CentralNodeTransport.open();

		TProtocol CentralNodeProtocol = new TBinaryProtocol(CentralNodeTransport);
		CentralNodeService.Client CentralNodeclient = new CentralNodeService.Client(CentralNodeProtocol);
		CentralNodeclient.returnMerge(myName.getIP(), myName.getPort(), FileName, "");
		CentralNodeTransport.close();
	}
	catch(TException xx){
		xx.printStackTrace();
	}
		MergeTime=MergeTime+System.currentTimeMillis()-time;
			displayStatistics();
}};
Runnable simple = new MergeThread(FileName, Priority, Parts,Merges);
numMergeJobs++;
new Thread(simple).start();

}

@Override
public boolean ping (boolean message){
	//try{
	//	Thread.sleep(10);
	//}catch(InterruptedException e){
	//}
  if(_FAIL_PROB_ == 0) return message;
  Random ran = new Random();
  int x = ran.nextInt(_FAIL_PROB_)-1;
  //x = 10/x;
  if(x == 0)
  {
    System.out.println("NODE FAILED.");
    System.exit(0);
  }
  for(;x<10; x++) message = true;

	return message;

}

public void Sort3(String FileName, int Priority, boolean Local){

	System.out.println("Sort received for file "+ FileName);

}

@Override
public void Sort(String FileName, int Priority, boolean Local){

	System.out.println("Sort received for file "+ FileName);
	class SortThread implements Runnable{
		String FileName;
		int Priority;
		boolean Local;
		public SortThread(String fn, int pri, boolean loc){
			FileName = fn;
			Priority = pri;
			Local = loc;
		}
		public void run() {
			long Time = System.currentTimeMillis();
			ArrayList<Integer> words = new ArrayList<Integer>();


			Scanner sc2 = null;
			try {
				sc2 = new Scanner(new File("InterDir/"+FileName));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			while (sc2.hasNextLine()) {
				Scanner s2 = new Scanner(sc2.nextLine());
				while (s2.hasNextInt()) {
					try{
						Integer s = s2.nextInt();
						words.add(s);
					}catch(Exception e){e.printStackTrace();}
					//System.out.println(s);
				}
			}

			Collections.sort(words);
			StringBuilder sb = new StringBuilder();
			for(Integer S : words){
				sb.append(S);
				sb.append(" ");
			}
			//System.out.println(sb.toString());
			if(Local == false){
				try (PrintWriter writer = new PrintWriter("InterDir/"+FileName, "UTF-8")){
					writer.println(sb.toString());
					//writer.println("Sorted");
				}catch(IOException e){

				}
			}
			else{
				try (PrintWriter writer = new PrintWriter("OutputDir/"+FileName, "UTF-8")){
					writer.println(sb.toString());
					//writer.println("Sorted");
				}catch(IOException e){

				}
			}
			if(Local == false){
				TTransport CentralNodeTransport;
				String CentralNodeAddr = "localhost";
				int CentralPort =9090;

				try(Scanner scan = new Scanner(new File("CentralNodeInfo.txt"))){
					while(scan.hasNextLine()){
						CentralNodeAddr=scan.nextLine();
						CentralPort = Integer.parseInt(scan.nextLine());
					}
				}
				catch(FileNotFoundException e ){

				}


				try{
					CentralNodeTransport = new TSocket(CentralNodeAddr, CentralPort); // csel-x29-10
					CentralNodeTransport.open();

					TProtocol CentralNodeProtocol = new TBinaryProtocol(CentralNodeTransport);
					CentralNodeService.Client CentralNodeclient = new CentralNodeService.Client(CentralNodeProtocol);
					CentralNodeclient.returnSort(myName.getIP(), myName.getPort(), FileName, "");
					CentralNodeTransport.close();
				}
				catch(TException xx){
					//xx.printStackTrace();
				}

			}

			SortTime = SortTime + System.currentTimeMillis()-Time;
			displayStatistics();
		}
	};

	numSortJobs++;
	Runnable simple = new SortThread(FileName, Priority, Local);
	new Thread(simple).start();

}

 public static int randInt(int min, int max) {


 Random rand = new Random();
 int randomNum = rand.nextInt((max - min) + 1) + min;

 return randomNum;
}

 public static ArrayList<Integer> uniquerands(int required, int total){
	 ArrayList<Integer> list = new ArrayList<Integer>();
	 ArrayList<Integer> output = new ArrayList<Integer>();
     for (int i=0; i<total; i++) {
         list.add(new Integer(i));
     }
     Collections.shuffle(list);
     for (int i=0; i<required; i++) {
         output.add(list.get(i));
     }

     return output;

 }

 public void displayStatistics(){

	 if(numSortJobs>0){
		 System.out.println("Statistics for Sort Jobs completed:");
		 System.out.println("Number Of Sort Jobs:	"+numSortJobs);
		 System.out.println("Average Execution Time Of Sort Jobs:	"+SortTime/numSortJobs);
	 }

	 if(numMergeJobs>0){
		 System.out.println("Statistics for Merge Jobs completed:");
		 System.out.println("Number Of Merge Jobs:	"+numMergeJobs);
		 System.out.println("Average Execution Time Of Merge Jobs:	"+MergeTime/numMergeJobs);
	 }


	 System.out.println("Statistics for Total Job completed:");
	 System.out.println("Number Of Total Jobs:	"+(numSortJobs+numMergeJobs));
	 System.out.println("Average Execution Time Of Total Jobs:	"+(SortTime+MergeTime)/(numSortJobs+numMergeJobs));


 }



}
