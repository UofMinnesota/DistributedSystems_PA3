namespace java project3  // defines the namespace

    typedef i32 int  //typedefs to get convenient names for your types

    service NodeService {  // defines the service to add two numbers
            bool Write(1:string Filename 2:string Contents), //defines a method
            string Read(1:string Filename), //defines a method
            bool UpdateDHT(1:string NodeList),
            string Merge(1:string Filename, 2:string Content1, 3:string Content2),
            string Sort(1:string Filename, 2:string Content)
    }
