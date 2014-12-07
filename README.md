Distributed_FS
==============

Implementing a distributed file system in java. There's 1 meta-server with no secondary meta-node. Support scalable file servers and clients, file server crash recovery & chunk replication(level = 3).

* meta server must be running on dc30
* meta server is designed to be always reliable. Once meta server crashes, all file information will be lost.
* on file server recovery, it will validate all local files according to metadata from meta server
* when doing write/append operation, a second write/append to the same filename will be blocked till the first one finishes(replication process also needs to be done for the first operation). 

## How to run
1. unzip archive
2. on any linux box, compile all source code: "javac *.java"
3. on dc30, start meta server: "java meta_server"
4. on any dc** machines, start file servers: "java file_server"
5. on any dc** machines, start client: "java client [input_file]"

## Note
* make sure you have a JDK version at least 1.7 to compile and run this program.
* there's no logs. all logs will be printed out to the standart output
* file server space limit is set to 32K bytes
