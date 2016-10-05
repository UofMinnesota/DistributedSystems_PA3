namespace java project3  // defines the namespace

    typedef i32 int  //typedefs to get convenient names for your types

    service ComputeNodeService {

            int getVersionNumber(1:string Filename),

            bool ping(1:bool message),


            void Merge(1:string Filename, 2:int Priority ,3:int Parts, 4:int merges),
            void Sort(1:string Filename, 2:int Priority, 3:bool local),


    }
