package project3;


import java.util.Collections;
import java.util.ArrayList;



// NodeName class contains IP address, port and ID of the nodes

public class NodeName implements Comparable<NodeName>{
	
	private String myIP;
	private int myPort;
	private int myID;
	static int idCount = 0;
	private ArrayList<String> mergeList;
	private ArrayList<String> sortList;

	public int compareTo(NodeName N) {
	        return Integer.compare(myID, N.myID);
	   }

	
	public void setID (int id) {
		this.myID = id;
	}
	public void setIP (String ip) {
		this.myIP = ip;
	}
	public void setPort (int port) {
		this.myPort = port;
	}
	public int getID() {
		return this.myID;
	}
	public String getIP() {
		return this.myIP;
	}
	public int getPort() {
		return this.myPort;
	}
	
	public void addMergeJob(String FileName){
		if(!mergeList.contains(FileName)){
			System.out.println("The file added for merge is "+FileName+ " Port is "+myPort);
			mergeList.add(FileName);
		}
	}
	public void addSortJob(String FileName){
		System.out.println("The file added for sort is "+FileName+ " Port is "+myPort);
		sortList.add(FileName);
	}
	
	public void removeSortJob(String FileName){
		sortList.remove(FileName);
		for(int i=0;i<sortList.size();i++){
			if(sortList.get(i).equals(FileName)){
				System.out.println("The file removed for sort is "+FileName+ " Port is "+myPort);
				sortList.remove(i);
			}
		}
	}
	
	public void removeMergeJob(String FileName){
		for(int i=0;i<mergeList.size();i++){
			if(mergeList.get(i).equals(FileName)){
				System.out.println("The file removed for merge is "+FileName+ " Port is "+myPort);
				mergeList.remove(i);
			}
		}
		
		
	}
	
	public ArrayList<String> getMergeList(){
		return mergeList;
	}
	
	public ArrayList<String> getSortList(){
		return sortList;
	}
	
	public NodeName(String ip, int port, int id){
		myID = id;
		myIP = ip;
		myPort = port;
		mergeList = new ArrayList<String>();
		sortList = new ArrayList<String>();
		
	}
	public NodeName(String ip, int port){
		myID = idCount++;
		myIP = ip;
		myPort = port;
		mergeList = new ArrayList<String>();
		sortList = new ArrayList<String>();
	}
}
