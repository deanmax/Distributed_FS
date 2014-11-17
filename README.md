Distributed_FS
==============

Implementing a distributed file system in java. Initially with 2 clients, 3 servers and 1 meta-server.

* meta server must be running on dc30
* meta server is designed to be always reliable. Once meta server crashes, all file information will be lost.
* on file server recovery, it will validate all local files according to metadata from meta server
* when doing write/append operation, a second write/append to the same filename will be blocked till the first one finishes or fails. 

## How to run
1. unzip archive
2. on any linux box, compile all source code: "javac *.java"
3. on dc30, start meta server: "java meta_server"
4. on any dc** machines, start file servers: "java file_server"
5. on any dc** machines, start client: "java client [input_file]"

## Note
* make sure you have a JDK version at least 1.7 to compile and run this program.
* there's no logs. all logs will be printed out to the standart output