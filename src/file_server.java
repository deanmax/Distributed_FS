import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


public class file_server {
	
	public static void main(String[] args) {
		final String m_server = "dc30";  // hardcode dc30 as metadata server
		
		// local file storage meta data
		// Format:
		// key: blk_name, value: blk_size
		final ConcurrentHashMap<String, Integer> file_meta = new ConcurrentHashMap<String, Integer>();
		
		
		// start a thread to send heartbeat to meta server
		new HeartBeat(file_meta).start();
		
		try {
			ServerSocket listenSocket = new ServerSocket(8822);
			System.out.println("Start listening on port 8822...");
			while (true) {
				Socket clientSocket = listenSocket.accept();
				String from_host = clientSocket.getInetAddress().getHostName().split("\\.")[0];
				ObjectInputStream input = new ObjectInputStream(clientSocket.getInputStream());
				
				OpsRequest req = (OpsRequest)input.readObject();
				System.out.println(req.type+ " request from " +from_host);
				new Operation(req, clientSocket, file_meta).start();  // each new thread handle one request
			}
		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Server Exception: "+e.getMessage());
		}
		
		
	}
}


class Operation extends Thread {
	OpsRequest req;
	Socket socket;
	ConcurrentHashMap<String, Integer> file_meta;
	
	
	public Operation(OpsRequest req, Socket socket, 
			ConcurrentHashMap<String, Integer> file_meta) {
		this.req = req;
		this.socket = socket;
		this.file_meta = file_meta;
	}
	
	public void run() {
		
		
		try {
			DataOutputStream output = new DataOutputStream(socket.getOutputStream());
			
			
			
			// update local meta data
			// atomic operation
			synchronized(file_meta) {
				
			}
			
			// send ops result back to client
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		

	}
}


class HeartBeat extends Thread {
	ConcurrentHashMap<String, Integer> file_meta;
	String hostname;
	
	public HeartBeat(ConcurrentHashMap<String, Integer> file_meta) {
		this.file_meta = file_meta;
		try {
			this.hostname = InetAddress.getLocalHost().getHostName().split("\\.")[0];
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void run() {
		// create subdir named with server hostname if there isn't one
		File folder = new File("./"+hostname);
		folder.mkdir();
		File[] listOfFiles = folder.listFiles();
	
		// delete all existing files
		for (File file : listOfFiles) {
			file.delete();
			/*
			if (!file.isFile()) continue;
		    	
			String filename = file.getName();
			// only want files with [filename]_[numeric] format
			if (filename.split("_").length == 1 || 
				!filename.split("_")[1].matches("^\\d+$")) continue;
			
			int file_size = (int) file.length();
			file_meta.put(filename, file_size);
			*/
		}
					
		while (true) {
			try {
				// send heartbeat to meta server
				String m_server = "dc30";  // hardcode dc30 as metadata server
				Socket m_s = new Socket(m_server + ".utdallas.edu", 8821);
				ObjectOutputStream m_output = new ObjectOutputStream(m_s.getOutputStream());
				
				// heart beat message is an ArrayList with each element having block_name and size
				ArrayList<MetaRequest> hb = new ArrayList<MetaRequest>();
				synchronized(file_meta) {
					for (Entry<String, Integer> e: file_meta.entrySet()) {
						hb.add(new MetaRequest(ReqType.HEARTBEAT, e.getKey(), e.getValue()));
					}
					m_output.writeObject(ReqType.HEARTBEAT);
					m_output.writeObject(hb);
				}
				m_s.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
