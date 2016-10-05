namespace java project3  // defines the namespace

    typedef i32 int  //typedefs to get convenient names for your types

    service FileService {
            //Interface for server from client
            bool clientWrite(1:string Filename 2:string Contents), //
            string clientRead(1:string Filename), //defines a method


            //Version numbers:
            //Enquire version number(get)
              int getVersionNumber(1:string Filename),


            string Merge(1:string Filename ),
            string Sort(1:string Filename),


    }
