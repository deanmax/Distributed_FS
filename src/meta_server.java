import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;



public class meta_server {
	
	public static void main(String[] args) {
		// meta data table
		ConcurrentHashMap<String, ArrayList<MetaData>> meta_table = 
				new ConcurrentHashMap<String, ArrayList<MetaData>>();
		
		try {			
			ServerSocket listenSocket = new ServerSocket(8821);
			System.out.println("Start listening on port 8821...");
			while (true) {
				Socket clientSocket = listenSocket.accept();
				String from_host = clientSocket.getInetAddress().getHostName().split("\\.")[0];
				ObjectInputStream input = new ObjectInputStream(clientSocket.getInputStream());
				ObjectOutputStream output = new ObjectOutputStream(clientSocket.getOutputStream());
				
				MetaRequest req = (MetaRequest) input.readObject();
				System.out.println(req.type+ " request from " +from_host);
				
				
			}
		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Exception: "+e.getMessage());
		}
	}
}

class MetaData {
	boolean isValid = true;
	int blk_id;
	int eff_length;
	long update_time;
	String filename;
	String f_server;
}