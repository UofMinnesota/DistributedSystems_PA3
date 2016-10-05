package project3;

import java.util.ArrayList;


public class FileStore {
	private String FileName;
	private int NumParts;
	private ArrayList<Integer> SortStatus;
	private  boolean MergeStatus;
	private String Status;
	//private int Version;
	
	FileStore(	String FN, int parts){
		System.out.println("Filestore entered");
		FileName = FN;
		NumParts = parts;
		SortStatus = new ArrayList<Integer>();
		MergeStatus = false;
		Status = "File Created";
		for(int i = 0; i<NumParts; i++){
			SortStatus.add(i, 0 );
		}
		System.out.println("Filestore created");
	}
	
	public String getFileName(){
		return FileName;
	}
	
	public int getNumParts(){
		return NumParts;
	}
	
	public boolean getMergeStatus(){
		return MergeStatus; 
	}
	
	public boolean getSortStatus(){
		
		int ss=0;
		
		for(int i =0; i<NumParts;i++){
			ss += SortStatus.get(i).intValue(); 
		}
		
		if(ss == NumParts)
			return true;
		else
			return false;
	}
	
	
	public void setSortStatus(int partNo, boolean comp){
		
		if(comp == false){
			SortStatus.set(partNo, 0);
		}
		else{
			SortStatus.set(partNo, 1);
		}
	}
	
	public void setMergeStatus(boolean comp){

			MergeStatus = comp;
	}
	
	public void setStatus(String S){
		Status = S;
	}
	
	public String getStatus(){
		return Status;
	}

	
}
