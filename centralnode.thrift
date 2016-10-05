namespace java project3 // defines the namespace   

    typedef i32 int  //typedefs to get convenient names for your types

    service CentralNodeService {  // defines the service to add two numbers
            //for compute nodes to join
            string Join(1:string IP, 2:int Port), //defines a method
            
            //for sort jobs sent to the server by the client
            string ClientSort(1:string Filename),

            void returnSort(1:string nodename, 2:int port, 3:string Filename, 4:string Time),
            void returnMerge(1:string nodename, 2:int port, 3:string Filename, 4:string Time),
      	    
            //for compute nodes to report health
            string Heartbeat(1:string Name, 2:int Port, 3:int Seq)
    }
