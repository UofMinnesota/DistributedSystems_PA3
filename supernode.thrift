namespace java project3 // defines the namespace   

    typedef i32 int  //typedefs to get convenient names for your types

    service SuperNodeService {  // defines the service to add two numbers
            string Join(1:string IP, 2:int Port, 3:bool isCoordinator), //defines a method
            string GetNodeList(),
	    string Split(1:string Filename, 2:string Content),
	    string Heartbeat(1:string Name, 2:int Port, 3:int Seq)
    }
