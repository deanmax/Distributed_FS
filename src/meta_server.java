import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;



public class meta_server {
	
	public static void main(String[] args) {
		// meta data table
		// Ex:
		// {filename, [MetaData, MetaData, ...]}
		final ConcurrentHashMap<String, List<MetaData>> meta_table = 
				new ConcurrentHashMap<String, List<MetaData>>();
		final List<String> alive_server = Collections.synchronizedList(new ArrayList<String>());
		
		
		// start meta data table validation process
		new Validator(meta_table, alive_server).start();
		
		ServerSocket listenSocket = null;
		try {
			listenSocket = new ServerSocket(8821);
			System.out.println("Start listening on port 8821...");
		} catch (IOException e) {
			System.out.println("Exception: Error opening port 8821. "+e.getMessage());
			System.exit(2);
		}
			
		
		while (true) {
			try {
				Socket clientSocket = listenSocket.accept();
				String from_host = clientSocket.getInetAddress().getHostName().split("\\.")[0];
				ObjectInputStream input = new ObjectInputStream(clientSocket.getInputStream());
				ObjectOutputStream output = new ObjectOutputStream(clientSocket.getOutputStream());
				
				ReqType t = (ReqType) input.readObject();
				long timestamp = System.currentTimeMillis();
				System.out.println(timestamp+ ": " +t+ " request from " +from_host);
				
				
				if (t == ReqType.HEARTBEAT) {
					@SuppressWarnings("unchecked")
					ArrayList<MetaRequest> hb = (ArrayList<MetaRequest>) input.readObject();
					if (!alive_server.contains(from_host)) alive_server.add(from_host);
					
					if (hb.size() == 0) continue;
					
					Iterator<MetaRequest> iter = hb.iterator();
					while (iter.hasNext()) {
						MetaRequest hb_msg = iter.next();
						String filename = hb_msg.filename.split("_")[0];
						int blk_id = Integer.parseInt(hb_msg.filename.split("_")[1]);
						//int file_length = hb_msg.length;  // file length will be maintained by write/append operation
						
						MetaData meta_to_update = meta_table.get(filename).get(blk_id);
						synchronized(meta_table) {
							synchronized(meta_to_update) {
								meta_to_update.isValid = true;
								meta_to_update.update_time = timestamp;
								meta_to_update.f_server = from_host;
							}
						}
					}
					
				}
				
				
				else if (t == ReqType.PROBE) {
					/*
					// - test -
					List<MetaData> meta_entries = Collections.synchronizedList(new ArrayList<MetaData>());
                    MetaData entry = new MetaData();
                    entry.filename = "asdf";
                    entry.blk_id = 0;
                    entry.f_server = "dc31";
                    meta_entries.add(entry);
                    meta_table.put("asdf", meta_entries);
                    */
					output.writeObject(meta_table);
				}
				
				
				else if (t == ReqType.CREATE) {
					MetaRequest req = (MetaRequest) input.readObject();
					
					// need to remove existing file entry
					String filename = req.filename;
					synchronized(meta_table) {
						meta_table.remove(filename);
					}
					
					int num_of_blks = req.length / 8192;
					int remainder = req.length % 8192;
					int elements = remainder>0 ? num_of_blks+1 : num_of_blks;
					
					String[] alloc_server;
					int[] alloc_length;
					
					if (alive_server.size() == 0) {
						alloc_server = new String[0];
						alloc_length = new int[0];
					} else {
						alloc_server = new String[elements];
						alloc_length = new int[elements];
						
						for (int i=0; i<elements; i++) {
							// choose a random alive server to write to
							int rand = (int)(Math.random()*alive_server.size());
							alloc_server[i] = alive_server.get(rand);
							alloc_length[i] = (i==elements-1) ? remainder : 8192;
						}
					}
					
					MetaResponse res = new MetaResponse();
					res.file_server = alloc_server;
					res.eff_length = alloc_length;
					output.writeObject(res);
					
					
					// need to wait for client reply job done to update meta_table
					MetaRequest confirm = (MetaRequest) input.readObject();
					
					if (confirm.type == ReqType.RESULT && confirm.result) {
						List<MetaData> meta_entries = Collections.synchronizedList(new ArrayList<MetaData>());
						for (int i=0; i<elements; i++) {
							MetaData entry = new MetaData();
							entry.f_server = alloc_server[i];
							entry.eff_length = alloc_length[i];
							entry.blk_id = i;
							entry.filename = filename;
							entry.update_time = timestamp;
							meta_entries.add(entry);
						}
						synchronized(meta_table) {
							meta_table.put(filename, meta_entries);
						}
					}
				}
				
				
				else if (t == ReqType.APPEND) {
					// need to wait for client reply job done to update meta_table
					
				}
				
				
				else if (t == ReqType.READ) {
					
				}
			} catch (IOException | ClassNotFoundException e) {
				System.out.println("Exception: "+e.getMessage());
		    }
		}
	}
}

// process that periodically validate meta data table.
// set record as invalid if latest update time is more
// than 15 secs ago.
class Validator extends Thread {
	ConcurrentHashMap<String, List<MetaData>> meta_table;
	List<String> alive_server;
	
	Validator(ConcurrentHashMap<String, List<MetaData>> meta_table,
			List<String> alive_server) {
		this.meta_table = meta_table;
		this.alive_server = alive_server;
	}
	
	public void run() {
		while(true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if (meta_table.isEmpty()) continue;
			
			long time = System.currentTimeMillis();
			
			for (Entry<String, List<MetaData>> e: meta_table.entrySet()) {
				if (e.getValue().size() == 0) {
					synchronized(meta_table) {
						meta_table.remove(e.getKey());
					}
					continue;
				}
				
				List<MetaData> blk_list = e.getValue();
				synchronized(blk_list) {
					Iterator<MetaData> it = blk_list.iterator();
					while(it.hasNext()) {
						MetaData d = it.next();
						if (time - d.update_time > 15000) {  // millsecond
							d.isValid = false;
							if (alive_server.contains(d.f_server)) alive_server.remove(d.f_server);
						}
					}
				}
			}
		}  // end while
	}
}
