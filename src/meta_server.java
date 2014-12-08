import java.io.DataInputStream;
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
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;



public class meta_server {
	
	public static void main(String[] args) {
		// meta data table
		// Ex:
		// {filename, [MetaData, MetaData, ...]}
		final ConcurrentHashMap<String, List<MetaData>> meta_table = 
				new ConcurrentHashMap<String, List<MetaData>>();
		// available server to create/append files
		// sorted on space left
		final TreeSet<DataNode> alive_server = new TreeSet<DataNode>();
		
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
					HeartBeatMsg hb_message = (HeartBeatMsg) input.readObject();
					ArrayList<MetaRequest> hb = hb_message.hb;
					DataNode node = hb_message.node;
					node.update_time = timestamp;
					
					// update alive servers
					// remove data nodes that have space less than 8192 bytes
					synchronized(alive_server) {
						TreeSet<DataNode> alive_server_temp = new TreeSet<DataNode>(alive_server);
						alive_server.clear();
						for (Iterator<DataNode> serv_it = alive_server_temp.iterator(); serv_it.hasNext(); ) {
							DataNode node_it = serv_it.next();
							if (node_it.name.equals(node.name)) continue;
							alive_server.add(node_it);
						}
						if (node.space_left >= 8192) {
							alive_server.add(node);
						} else {
							System.out.println("[Warning] File Server "+node.name+" Out of Space!");
						}
					}
					
					/*
					// - Test -
					for (DataNode n : alive_server) {
                        System.out.print(n+", ");
                    }
                    System.out.println();
                    for (Entry<String, List<MetaData>> e: meta_table.entrySet()) {
                        System.out.println(e.getKey()+": "+e.getValue());
                    }
					*/
					
					if (hb.size() == 0) continue;
					
					Iterator<MetaRequest> iter = hb.iterator();
					NEXT: while (iter.hasNext()) {
						MetaRequest hb_msg = iter.next();
						String filename = hb_msg.filename.split("_")[0];
						int blk_id = Integer.parseInt(hb_msg.filename.split("_")[1]);
						int file_length = hb_msg.length;  // file length
						
						MetaData meta_to_update = meta_table.get(filename).get(blk_id);
						synchronized(meta_table) {
							synchronized(meta_to_update) {
								meta_to_update.eff_length = file_length;
								for (DataNode n : meta_to_update.f_server.values()) {
									if (n.name.equals(from_host)) {
										n.isValid = true;
										n.update_time = timestamp;
										continue NEXT;
									}
								}
								
								// no replica server found in metadata
								// must be a new node
								meta_to_update.f_server.put(from_host, node);
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
					String filename = req.filename;
					OpsRequest purge_req = new OpsRequest(ReqType.PURGE, filename);

                    if (meta_table.containsKey(filename)) {
                        synchronized(meta_table) {
                            // iterate all blocks of given file, send purge request to all replica servers
                            Iterator<MetaData> it = meta_table.get(filename).iterator();
                            while (it.hasNext()) {
                                MetaData entry = it.next();
                                for (String f_server: entry.f_server.keySet()) {
                                	Socket s = new Socket( f_server+".utdallas.edu", 8822);
                                    ObjectOutputStream f_output = new ObjectOutputStream(s.getOutputStream());
                                    f_output.writeObject(purge_req);
                                }
                            }

                            // remove local metadata entry
                            meta_table.remove(filename);
                        }
                    }
					
					int num_of_blks = req.length / 8192;
					int remainder = req.length % 8192;
					int elements = remainder>0 ? num_of_blks+1 : num_of_blks;
					
					ArrayList<ArrayList<String>> alloc_server = new ArrayList<ArrayList<String>>();
					int[] alloc_length;
					
					if (alive_server.size() < 3) {  // need at least 3 servers to do replicate
						alloc_length = new int[0];
					} else {
						alloc_length = new int[elements];
						
						for (int i=0; i<elements; i++) {
							ArrayList<String> replica_server = new ArrayList<String>();
							// choose up to 3 alive servers to write to(order by space left, descending)
							TreeSet<DataNode> avail_server_by_space = new TreeSet<DataNode>(alive_server);
							int size = avail_server_by_space.size();
							for (int j=0; j<(size>3?3:size); j++) {
								replica_server.add(avail_server_by_space.pollLast().name);
							}
							alloc_server.add(replica_server);
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
						for (int blk_id=0; blk_id<elements; blk_id++) {
							MetaData entry = new MetaData();
							
							for (int i=0; i<alloc_server.get(blk_id).size(); i++) {
								DataNode node = new DataNode();
								node.name = alloc_server.get(blk_id).get(i);
								node.update_time = timestamp;
								node.isValid = true;
								entry.f_server.put(node.name, node);
							}
							
							entry.eff_length = alloc_length[blk_id];
							entry.blk_id = blk_id;
							entry.filename = filename;
							
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
					
					// check to-append block availability
					// if all replicas are unavailable, skip append operation
					MetaData last_block = meta_table.get(filename).get(meta_table.get(filename).size()-1);
					if (last_block.getValidReplicas().size() == 0) {
						output.writeObject(new MetaResponse());
						continue;
					}
					
					ArrayList<ArrayList<String>> al_f_server = new ArrayList<ArrayList<String>>();
					ArrayList<Integer> al_append_len         = new ArrayList<Integer>();
					int blk_to_start = meta_table.get(filename).size() - 1;
					
					if (alive_server.size() < 3) {  // need at least 3 servers to do replicate
						al_append_len.add(0);
					} else {
						if (meta_table.get(filename).get(blk_to_start).eff_length == 8192) {
							blk_to_start += 1;
						} else if (meta_table.get(filename).get(blk_to_start).eff_length < 8192) {
							int remain_size = 8192 - meta_table.get(filename).get(blk_to_start).eff_length;
							
							if (remain_size >= append_len) {
								append_len = 0;
							} else {
								append_len -= remain_size / 2048 * 2048;
							}
							
							ArrayList<String> append_server = 
									new ArrayList<String>(meta_table.get(filename).get(blk_to_start).f_server.keySet());
							al_f_server.add(append_server);
							al_append_len.add(remain_size);
						} else {
							// should never goto here
							System.out.println("Fatal Error: block size larger than 8192 on server"+
									meta_table.get(filename).get(blk_to_start).f_server+"!");
							System.exit(255);
						}
						
						// create new file for the rest of append
						for (; append_len > 0; append_len -= 8192) {
							ArrayList<String> replica_server = new ArrayList<String>();
							// choose up to 3 alive servers to write to(order by space left, descending)
							TreeSet<DataNode> avail_server_by_space = new TreeSet<DataNode>(alive_server);
							int size = avail_server_by_space.size();
							for (int j=0; j<(size>3?3:size); j++) {
								replica_server.add(avail_server_by_space.pollLast().name);
							}
							al_f_server.add(replica_server);
							al_append_len.add((append_len < 8192) ? append_len: 8192);
						}
					}
					
					MetaResponse res = new MetaResponse();
					res.start_blk_id = blk_to_start;
					res.file_server  = al_f_server;
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
									for (DataNode d : meta_table.get(filename).get(blk_to_start).f_server.values()) {
										if (d.isValid) d.update_time = timestamp;
									}
									continue;
								}
								
								MetaData entry = new MetaData();
								for (int j=0; j<al_f_server.get(i).size(); j++) {
									DataNode node = new DataNode();
									node.name = al_f_server.get(i).get(j);
									node.update_time = timestamp;
									node.isValid = true;
									entry.f_server.put(node.name, node);
								}
								
								entry.eff_length = al_append_len.get(i);
								entry.blk_id = blk_to_start + i;
								entry.filename = filename;
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
					ArrayList<ArrayList<String>> al_f_server = new ArrayList<ArrayList<String>>();
					ArrayList<Integer> al_start_pos          = new ArrayList<Integer>();
					ArrayList<Integer> al_read_length        = new ArrayList<Integer>();
					
					for (int i = (req.length + req.pos % 8192), j = 0; i > 0; i -= 8192, j++) {
						if (meta_table.get(filename).get(start_blk_id+j).getValidReplicas().size() == 0) {
							output.writeObject(res);
							break;
						}
						al_f_server.add(meta_table.get(filename).get(start_blk_id+j).getValidReplicas());
						
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
					
					res.file_server  = al_f_server;
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
	final TreeSet<DataNode> alive_server;
	
	Validator(ConcurrentHashMap<String, List<MetaData>> meta_table,
			TreeSet<DataNode> alive_server) {
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
			
			long curr_time = System.currentTimeMillis();
			
			// check metadata update time
			// set metadata to false if that replica server is not responding for 15 secs,
			// and remove that replica server out of alive_server
			synchronized(meta_table) {
				for (Entry<String, List<MetaData>> e: meta_table.entrySet()) {
					if (e.getValue().size() == 0) {
						meta_table.remove(e.getKey());
						continue;
					}
					
					List<MetaData> blk_list = e.getValue();
					Iterator<MetaData> it = blk_list.iterator();
					while(it.hasNext()) {
						MetaData d = it.next();
						for (DataNode node : d.f_server.values()) {
							if (curr_time - node.update_time > 15000) {  // millsecond
								node.isValid = false;
								
								// remove server from alive_server set
								synchronized (alive_server) {
									TreeSet<DataNode> alive_server_temp = new TreeSet<DataNode>(alive_server);
									alive_server.clear();
									for (Iterator<DataNode> serv_it = alive_server_temp.iterator(); serv_it.hasNext(); ) {
										DataNode node_avail = serv_it.next();
										if (node_avail.name.equals(node.name)) continue;
										alive_server.add(node_avail);
									}
								}
								
							}
						}
					}
				}
			}
			
			// check metadata table again,
			// 1. remove invalid datanode
			// 2. create new file replica if replicas  < 3.
			// if all datanodes for a block file are invalid, all will be kept
			// until one replica server recover, the rest of them will be deleted
			synchronized(meta_table) {
				for (Entry<String, List<MetaData>> e: meta_table.entrySet()) {
					List<MetaData> blk_list = e.getValue();
					Iterator<MetaData> it = blk_list.iterator();
					while(it.hasNext()) {
						MetaData d = it.next();
						String blk = d.filename + "_" + d.blk_id;
						
						if (d.getValidReplicas().size() == 0) continue;
						
						String primary = d.getValidReplicas().get(0);
						
						ArrayList<DataNode> collection = new ArrayList<DataNode>(d.f_server.values());
						for (DataNode node : collection) {
							// delete invalid replica as long as there's at least one valid replica
							if (!node.isValid && d.getValidReplicas().size() > 0) {
								d.f_server.remove(node.name);
							}
						}
						
						// create backup replica if replica < 3
						ArrayList<String> exist_replicas = d.getValidReplicas();
						ArrayList<String> to_replica = new ArrayList<String>();
						for (int i=d.getValidReplicas().size(); i<3; i++) {
							if (alive_server.size() == 0) break;
							
							TreeSet<DataNode> avail_server_by_space = new TreeSet<DataNode>(alive_server);
							DataNode node = avail_server_by_space.pollLast();
							
							while (node != null && exist_replicas.size() < 3) {
								if (exist_replicas.contains(node.name)) {
									// replica server already used, try next available server from pool
									node = avail_server_by_space.pollLast();
									continue;
								} else {
									to_replica.add(node.name);
									exist_replicas.add(node.name);
									break;
								}
							}
						}
						
						
						//System.out.println("Blk: "+blk);
                        //System.out.println("To Replica: "+to_replica);
						if (!to_replica.isEmpty()) {
							OpsRequest req = new OpsRequest(ReqType.REPLICATE, blk, to_replica.toArray(new String[to_replica.size()]));
							try {
								// setup socket connection to file server
								Socket s = new Socket(primary + ".utdallas.edu", 8822);
								ObjectOutputStream f_output = new ObjectOutputStream(s.getOutputStream());
								f_output.writeObject(req);
								
								DataInputStream f_input = new DataInputStream(s.getInputStream());
								char f_response = f_input.readChar();
								
								// replication finished successfully, update metadata accordingly
								// replica server data node info is just a place holder,
								// it will be updated by next heartbeat message
								if (f_response == 'y') {
									for (String replica_server : to_replica) {
										d.f_server.put(replica_server, new DataNode(replica_server));
									}
								}
								
							} catch (Exception ex) {
								//ex.printStackTrace();
								System.out.println("Error communicating with replica server " + primary);
							}
						}
						
						
						
						
					}
				}
			}
			
		}  // end while
	}
}
