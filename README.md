Distributed_FS
==============

Implementing a distributed file system in java. Initially with 2 clients, 3 servers and 1 meta-server.

1. a second write/append to the same filename will be blocked till the first one finishes or fails.
2. a second write operation on the same file will urge meta server to send PURGE request to file servers, asking them to delete previous files

Note: make sure you have a JDK version at least 1.7 to compile and run this program.