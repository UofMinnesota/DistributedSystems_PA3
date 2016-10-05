###### WARNING ######
Change USE_LOCAL in both FilesClient.java and Node.java to true for localhost

How to compile:
	$ source compile

How to run supernode:
	$ source run_supernode
		or
	$ java -cp .:./jars/libthrift-0.9.1.jar:./jars/slf4j-api-1.7.14.jar project1.SuperNode <MAX_NODES>

How to run nodes:
	$ source run_node
		or
	$ java -cp .:./jars/libthrift-0.9.1.jar:./jars/slf4j-api-1.7.14.jar project1.Node <MAX_NODES>

How to run clients:
	For default test:
		$ source run_client 
	For write/read test:
		$ source run_client_test1
	For read-only test:
		$ source run_client_test2
	For write followed by reverse order read test:
		$ source run_client_test3
	For write followed by read of different file test:
		$ source run_client_test2
	
