import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


public class file_server {
	
	public static void main(String[] args) {
		// local file storage meta data
		// Format:
		// key: blk_name, value: blk_size
		final ConcurrentHashMap<String, Integer> file_meta = new ConcurrentHashMap<String, Integer>();
		
		final String m_server = "dc30";  // hardcode dc30 as metadata server
		String hostname = "";
		try {
			hostname = InetAddress.getLocalHost().getHostName().split("\\.")[0];
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(2);
		}
		
		
		// on start, probe meta server for local file validation
		try {
			Socket m_s = new Socket(m_server + ".utdallas.edu", 8821);
			
			ObjectOutputStream m_output = new ObjectOutputStream(m_s.getOutputStream());
			ObjectInputStream m_input = new ObjectInputStream(m_s.getInputStream());
			
			m_output.writeObject(ReqType.PROBE);
			@SuppressWarnings("unchecked")
			ConcurrentHashMap<String, List<MetaData>> meta_table = 
					(ConcurrentHashMap<String, List<MetaData>>) m_input.readObject();
			
			
			File folder = new File("./"+hostname);
			// create if there isn't one
			folder.mkdir();
			File[] listOfFiles = folder.listFiles();
			
			// delete invalid local files
			OUTER: for (File file : listOfFiles) {
				Iterator<String> it = meta_table.keySet().iterator();
				if (!it.hasNext()) {
					file.delete();
					continue;
				}
				
				while (it.hasNext()) {
					String key = it.next();
					Iterator<MetaData> iter = meta_table.get(key).iterator();
					while (iter.hasNext()) {
						MetaData entry = iter.next();
						
						if (entry.f_server.equals(hostname) && 
								file.getName().equals(entry.filename+"_"+entry.blk_id)) {
							synchronized(file_meta) {
								file_meta.put(entry.filename+"_"+entry.blk_id, entry.eff_length);
							}
							continue OUTER;
						}
					}
				}
				
				file.delete();
			}
			
		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Error contacting meta server!");
			System.exit(2);
		}
		
		
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
				new Operation(req, clientSocket, file_meta, hostname).start();  // each new thread handle one request
			}
		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Server Exception: "+e.getMessage());
		}
		
	} // end of main
}


class Operation extends Thread {
	OpsRequest req;
	Socket socket;
	ConcurrentHashMap<String, Integer> file_meta;
	String hostname;
	
	
	public Operation(OpsRequest req, Socket socket, 
			ConcurrentHashMap<String, Integer> file_meta, String hostname) {
		this.req = req;
		this.socket = socket;
		this.file_meta = file_meta;
		this.hostname = hostname;
	}
	
	public void run() {
		
		// write request
		if (req.type == ReqType.CREATE) {
			String blk = req.block;
			String filename = blk.split("_")[0];
			String text = req.text;
			
			// remove all local metadata regarding requested file block
			synchronized(file_meta) {
				Iterator<String> iter = file_meta.keySet().iterator();
				while (iter.hasNext()) {
					String key = iter.next();
					if (key.split("_")[0].equals(filename)) {
						file_meta.remove(key);
					}
				}
			}
			
			boolean ops_fail = false;
			try {
				FileWriter fw = new FileWriter("./"+hostname+"/"+blk, false);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(text);
				bw.flush();
				bw.close();
				
				// update local meta data
				// atomic operation
				synchronized(file_meta) {
					file_meta.put(blk, text.length());
				}
			} catch (IOException e) {
				ops_fail = true;
				e.printStackTrace();
			}
			
			// send ops result back to client
			try {
				DataOutputStream to_client = new DataOutputStream(socket.getOutputStream());
				if (ops_fail) {
					to_client.writeChar('n');
				} else {
					to_client.writeChar('y');
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// append request
		else if (req.type == ReqType.APPEND) {
			String blk = req.block;
			String text = req.text;
			boolean ops_fail = false;
			try {
				FileWriter fw = new FileWriter("./"+hostname+"/"+blk, true);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(text);
				bw.flush();
				bw.close();
				
				// update local meta data
				// atomic operation
				synchronized(file_meta) {
					if (file_meta.containsKey(blk)) {
						int old_len = file_meta.get(blk);
						file_meta.replace(blk, old_len+text.length());
					} else {
						file_meta.put(blk, text.length());
					}
				}
			} catch (IOException e) {
				ops_fail = true;
				e.printStackTrace();
			}
			
			// send ops result back to client
			try {
				DataOutputStream to_client = new DataOutputStream(socket.getOutputStream());
				if (ops_fail) {
					to_client.writeChar('n');
				} else {
					to_client.writeChar('y');
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// read request
		else if (req.type == ReqType.READ) {
			String blk = req.block;
			int pos = req.pos;
			int read_length = req.read_length;
			char[] buffer = new char[read_length];
			boolean ops_fail = false;
			
			FileReader fr;
			try {
				fr = new FileReader("./"+hostname+"/"+blk);
				BufferedReader br = new BufferedReader(fr);
				br.skip(pos);
				br.read(buffer, 0, read_length);
				br.close();
			} catch (IOException e) {
				ops_fail = true;
				e.printStackTrace();
			}
			
			// send ops result back to client
			try {
				ObjectOutputStream to_client = new ObjectOutputStream(socket.getOutputStream());
				if (ops_fail) {
					to_client.writeObject("");
				} else {
					to_client.writeObject(String.valueOf(buffer));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
}


class HeartBeat extends Thread {
	ConcurrentHashMap<String, Integer> file_meta;
	
	public HeartBeat(ConcurrentHashMap<String, Integer> file_meta) {
		this.file_meta = file_meta;
	}
	
	public void run() {
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
					//m_input.skip(m_input.available());
					//while(m_input.available() == 0) {}  // wait for meta server reply to terminate connection
				}
				//m_s.close();
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
