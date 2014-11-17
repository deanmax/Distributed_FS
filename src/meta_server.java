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
				ObjectOutputStream output = new ObjectOutputStream(clientSocket.getOutputStream());
				ObjectInputStream input = new ObjectInputStream(clientSocket.getInputStream());
				
				ReqType t = (ReqType) input.readObject();
				long timestamp = System.currentTimeMillis();
				System.out.println(timestamp+ ": " +t+ " request from " +from_host);
				
				
				if (t == ReqType.HEARTBEAT) {
					@SuppressWarnings("unchecked")
					ArrayList<MetaRequest> hb = (ArrayList<MetaRequest>) input.readObject();
					if (!alive_server.contains(from_host)) alive_server.add(from_host);
					
					/*
					// - Test -
					for (String s : alive_server) {
                        System.out.print(s+", ");
                    }
                    System.out.println();
					*/
					
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
					MetaRequest req = (MetaRequest) input.readObject();
					String filename = req.filename;
					int append_len = req.length;
					
					if (!meta_table.containsKey(filename)) {
						output.writeObject(new MetaResponse());
						continue;
					}
					
					// check all blocks availability
					boolean valid = true;
					synchronized(meta_table) {
						Iterator<MetaData> it = meta_table.get(filename).iterator();
						while (it.hasNext()) {
							MetaData entry = it.next();
							if (!entry.isValid) {
								valid = false;
								break;
							}
						}
					}
					if (!valid) {
						output.writeObject(new MetaResponse());
						continue;
					}
					
					
					ArrayList<String> al_f_server     = new ArrayList<String>();
					ArrayList<Integer> al_append_len  = new ArrayList<Integer>();
					
					int blk_to_start = meta_table.get(filename).size() - 1;
					if (meta_table.get(filename).get(blk_to_start).eff_length == 8192) {
						blk_to_start += 1;
					} else if (meta_table.get(filename).get(blk_to_start).eff_length < 8192) {
						int remain_size = 8192 - meta_table.get(filename).get(blk_to_start).eff_length;
						append_len -= remain_size / 2048 * 2048;
						al_f_server.add(meta_table.get(filename).get(blk_to_start).f_server);
						al_append_len.add(remain_size);
					} else {
						// should never goto here
						System.out.println("Fatal Error: block size larger than 8192 on server"+
								meta_table.get(filename).get(blk_to_start).f_server+"!");
						System.exit(255);
					}
					
					for (; append_len > 0; append_len -= 8192) {
						int rand = (int)(Math.random()*alive_server.size());
						al_f_server.add(alive_server.get(rand));
						al_append_len.add((append_len < 8192) ? append_len: 8192);
					}
					
					MetaResponse res = new MetaResponse();
					res.start_blk_id = blk_to_start;
					res.file_server  = al_f_server.toArray(new String[al_f_server.size()]);
					res.eff_length   = new int[al_append_len.size()];
					for (int i = 0; i < al_append_len.size(); i++) {
						res.eff_length[i] = al_append_len.get(i);
					}
					output.writeObject(res);
					
					// need to wait for client reply job done to update meta_table
					MetaRequest confirm = (MetaRequest) input.readObject();
					
					if (confirm.type == ReqType.RESULT && confirm.result) {
						synchronized(meta_table) {
							for (int i = 0; i < al_f_server.size(); i++) {
								if (meta_table.get(filename).size() == blk_to_start + i + 1) {
									// update existing record (last block)
									if (req.length < res.eff_length[0]) {
										meta_table.get(filename).get(blk_to_start).eff_length += req.length;
									} else {
										meta_table.get(filename).get(blk_to_start).eff_length = 8192;
									}
									meta_table.get(filename).get(blk_to_start).update_time = timestamp;
									continue;
								}
								
								MetaData entry = new MetaData();
								entry.f_server = al_f_server.get(i);
								entry.eff_length = al_append_len.get(i);
								entry.blk_id = blk_to_start + i;
								entry.filename = filename;
								entry.update_time = timestamp;
								meta_table.get(filename).add(entry);
							}
						}
					}
					
				}
				
				
				else if (t == ReqType.READ) {
					MetaRequest req = (MetaRequest) input.readObject();
					
					String filename = req.filename;
					int req_length = req.length;  // read length from request
					
					if (!meta_table.containsKey(filename)) {
						// reply with empty response
						MetaResponse res = new MetaResponse();
						output.writeObject(res);
						continue;
					}
					
					int start_blk_id = req.pos / 8192;
					
					/*
					if (req.pos % 8192 + req.length <= 8192) {
						MetaResponse res = new MetaResponse();
						// if file server down, reply with empty response
						if (!meta_table.get(filename).get(start_blk_id).isValid) {
							output.writeObject(res);
							continue;
						}
						
						f_server = new String[] {meta_table.get(filename).get(start_blk_id).f_server};
						start_pos = new int[] {req.pos % 8192};
						read_length = new int[] {req.length};
						res.file_server  = f_server;
						res.eff_length   = read_length;
						res.pos          = start_pos;
						res.start_blk_id = start_blk_id;
						
						output.writeObject(res);
						continue;
					*/
					
					MetaResponse res = new MetaResponse();
					ArrayList<String> al_f_server     = new ArrayList<String>();
					ArrayList<Integer> al_start_pos   = new ArrayList<Integer>();
					ArrayList<Integer> al_read_length = new ArrayList<Integer>();
					
					for (int i = (req.length + req.pos % 8192), j = 0; i > 0; i -= 8192, j++) {
						if (!meta_table.get(filename).get(start_blk_id+j).isValid) {
							output.writeObject(res);
							break;
						}
						al_f_server.add(meta_table.get(filename).get(start_blk_id+j).f_server);
						
						if (j == 0) {
							al_start_pos.add(req.pos % 8192);
							int act_len = (req_length + req.pos % 8192 - 8192) > 0 ? (8192 - req.pos % 8192) : req_length;
							req_length -= act_len;
							al_read_length.add(act_len);
						} else {
							al_start_pos.add(0);
							al_read_length.add((req_length - 8192) > 0 ? 8192 : req_length);
							req_length -= 8192;
						}
					}
					
					res.file_server  = al_f_server.toArray(new String[al_f_server.size()]);
					res.eff_length   = new int[al_read_length.size()];
					res.pos          = new int[al_start_pos.size()];
					res.start_blk_id = start_blk_id;
					
					for (int i = 0; i < al_start_pos.size(); i++) {
						res.eff_length[i] = al_read_length.get(i);
						res.pos[i] = al_start_pos.get(i);
					}
					
					output.writeObject(res);
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
