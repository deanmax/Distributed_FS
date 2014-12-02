import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;



public class client {
	
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("\tUsage: client input_file");
			System.exit(2);
		}
		
		String input_file = args[0];
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(input_file));
		} catch (FileNotFoundException e1) {
			System.out.println("Error open input file!");
			e1.printStackTrace();
			System.exit(1);
		}
		
		try {
			String line;
			// input file readline
			while ((line = br.readLine()) != null) {
				if (line.isEmpty()) continue;
				
				String m_server = "dc30";  //hardcode dc30 as metadata server
				Socket m_s = new Socket(m_server + ".utdallas.edu", 8821);  // socket connection to metadata server
				ObjectOutputStream m_output = new ObjectOutputStream(m_s.getOutputStream());
			
				
				String ops = line.split("\\|")[0];
				String filename = line.split("\\|")[1];
				System.out.println("Ops: " +ops+ " " +filename);
				
				///////////////////
				// write operation
				///////////////////
				if (ops.equalsIgnoreCase("w")) {
					String text = line.split("\\|")[2];
					
					m_output.writeObject(ReqType.CREATE);
					MetaRequest w_req = new MetaRequest(ReqType.CREATE, filename, text.length());
					m_output.writeObject(w_req);
					
					ObjectInputStream m_input = new ObjectInputStream(m_s.getInputStream());
					MetaResponse meta_res = (MetaResponse) m_input.readObject();
					
					// check allocated file servers
					if (meta_res.file_server.size() == 0) {
						System.out.println("No file server available according to meta-server, write skipped...");
						m_output.writeObject(new MetaRequest(ReqType.RESULT, false));
						Thread.sleep(2000);
						continue;
					}
					
					// send write request to file server
					boolean write_result = true;
					for (int i = 0; i < meta_res.file_server.size(); i++) {
						ArrayList<String> alloc_server = meta_res.file_server.get(i);
						String primary = alloc_server.remove(0);  // first server is primary
						String[] replica_server = alloc_server.toArray(new String[alloc_server.size()]);
						String blk = filename + "_" + i;
						String sub_text = text.substring(i*8192, i*8192+meta_res.eff_length[i]);
						
						OpsRequest req = new OpsRequest(ReqType.CREATE, blk, sub_text, replica_server);
						
						boolean redo = false;  // flag indicating if file server operation succeeds or not
						int retry = 0;
						do {
							try {
								System.out.println("Write " +blk+ "(" +meta_res.eff_length[i]+ 
										") to file server " +primary);
								// setup socket connection to file server
								Socket s = new Socket(primary + ".utdallas.edu", 8822);
								ObjectOutputStream f_output = new ObjectOutputStream(s.getOutputStream());
								DataInputStream f_input = new DataInputStream(s.getInputStream());
	                			
								f_output.writeObject(req);
								char f_response = f_input.readChar();
								
								if (f_response == 'n') {
									redo = true;
								} else {
									redo = false;
								}
							} catch (Exception ex) {
								//ex.printStackTrace();
								System.out.println("Error communicating with file server " + primary);
								redo = true;
							}
							
							if (redo) {
								System.out.println("FAILED: " + "RETRY " + retry++);
								Thread.sleep(2000);
							}
            			
						} while (redo == true && retry < 3);
            			
						if (retry == 3) {
							System.out.println("Write FAILED!");
							write_result = false;
						}
					}
					
					// reply write result back to meta server
					MetaRequest result = new MetaRequest(ReqType.RESULT, write_result);
					m_output.writeObject(result);
					
				} // end write operation
				
				
				////////////////////
				// append operation
				////////////////////
				else if (ops.equalsIgnoreCase("a")) {
					String text = line.split("\\|")[2];
					
					m_output.writeObject(ReqType.APPEND);
					MetaRequest a_req = new MetaRequest(ReqType.APPEND, filename, text.length());
					m_output.writeObject(a_req);
					
					ObjectInputStream m_input = new ObjectInputStream(m_s.getInputStream());
					MetaResponse meta_res = (MetaResponse) m_input.readObject();
					
					// check allocated file servers
					if (meta_res.file_server.size() == 0) {
						System.out.println("File unavailable according to meta-server, append skipped...");
						m_output.writeObject(new MetaRequest(ReqType.RESULT, false));
						Thread.sleep(2000);
						continue;
					}
					
					
					// send append request to file server
					boolean append_result = true;
					
					//// append will not cross file
					if (meta_res.file_server.size() == 1) {
						ArrayList<String> alloc_server = meta_res.file_server.get(0);
						String primary = alloc_server.remove(0);  // first server is primary
						String[] replica_server = alloc_server.toArray(new String[alloc_server.size()]);
						String blk = filename + "_" + meta_res.start_blk_id;
						OpsRequest req = new OpsRequest(ReqType.APPEND, blk, text, replica_server);
						
						boolean redo = false;  // flag indicating if file server operation succeeds or not
						int retry = 0;
						do {
							try {
								System.out.println("Append to " +blk+ "(" +text.length()+ 
										") on file server " +primary);
								// setup socket connection to file server
								Socket s = new Socket(primary + ".utdallas.edu", 8822);
								ObjectOutputStream f_output = new ObjectOutputStream(s.getOutputStream());
								DataInputStream f_input = new DataInputStream(s.getInputStream());
	                			
								f_output.writeObject(req);
								char f_response = f_input.readChar();
								
								if (f_response == 'n') {
									redo = true;
								} else {
									redo = false;
								}
							} catch (Exception ex) {
								//ex.printStackTrace();
								System.out.println("Error communicating with file server " + primary);
								redo = true;
							}
							
							if (redo) {
								System.out.println("FAILED: " + "RETRY " + retry++);
								Thread.sleep(2000);
							}
            			
						} while (redo == true && retry < 3);
            			
						if (retry == 3) {
							System.out.println("Append FAILED!");
							append_result = false;
						}
					
					// append will cross file
					} else {
						for (int i = 0; i < meta_res.file_server.size(); i++) {
							ArrayList<String> alloc_server = meta_res.file_server.get(i);
							String primary = alloc_server.remove(0);  // first server is primary
							String[] replica_server = alloc_server.toArray(new String[alloc_server.size()]);
							String blk = filename + "_" + (meta_res.start_blk_id + i);
							int actual_bytes = 0, null_bytes = 0;
							
							if (i == 0) {
								actual_bytes = meta_res.eff_length[i] / 2048 * 2048;
								null_bytes = meta_res.eff_length[i] - actual_bytes;
								
							} else {
								actual_bytes = meta_res.eff_length[i];
								null_bytes = 0;
							}
							String sub_text = text.substring(0, actual_bytes);
							OpsRequest req = new OpsRequest(ReqType.APPEND, blk, sub_text, replica_server);
							
							boolean redo = false;  // flag indicating if file server operation succeeds or not
							int retry = 0;
							do {
								if (actual_bytes == 0) break;
								
								try {
									System.out.println("Append to " +blk+ "(" +sub_text.length()+ 
											") on file server " +primary);
									// setup socket connection to file server
									Socket s = new Socket(primary + ".utdallas.edu", 8822);
									ObjectOutputStream f_output = new ObjectOutputStream(s.getOutputStream());
									DataInputStream f_input = new DataInputStream(s.getInputStream());
		                			
									f_output.writeObject(req);
									char f_response = f_input.readChar();
									
									if (f_response == 'n') {
										redo = true;
									} else {
										redo = false;
									}
								} catch (Exception ex) {
									//ex.printStackTrace();
									System.out.println("Error communicating with file server " + primary);
									redo = true;
								}
								
								if (redo) {
									System.out.println("FAILED: " + "RETRY " + retry++);
									Thread.sleep(2000);
								}
	            			
							} while (redo == true && retry < 3);
	            			
							if (retry == 3) {
								System.out.println("Append FAILED!");
								append_result = false;
								break;
							}
							
							// append null characters
							if (null_bytes != 0) {
								char[] null_chars = new char[null_bytes];
								for (int j = 0; j < null_chars.length; j++) {
									null_chars[j] = '\0';
								}
								
								req = new OpsRequest(ReqType.APPEND, blk, new String(null_chars), replica_server);
								
								redo = false;  // flag indicating if file server operation succeeds or not
								retry = 0;
								do {
									try {
										System.out.println("Append null characters to " +blk+ "(" +null_chars.length+ 
												") on file server " +primary);
										// setup socket connection to file server
										Socket s = new Socket(primary + ".utdallas.edu", 8822);
										ObjectOutputStream f_output = new ObjectOutputStream(s.getOutputStream());
										DataInputStream f_input = new DataInputStream(s.getInputStream());
			                			
										f_output.writeObject(req);
										char f_response = f_input.readChar();
										
										if (f_response == 'n') {
											redo = true;
										} else {
											redo = false;
										}
									} catch (Exception ex) {
										//ex.printStackTrace();
										System.out.println("Error communicating with file server " + primary);
										redo = true;
									}
									
									if (redo) {
										System.out.println("FAILED: " + "RETRY " + retry++);
										Thread.sleep(2000);
									}
		            			
								} while (redo == true && retry < 3);
		            			
								if (retry == 3) {
									System.out.println("Append FAILED!");
									append_result = false;
									break;
								}
							}
							
							text = text.substring(actual_bytes);
						}
					}
					
					
					// reply write result back to meta server
					MetaRequest result = new MetaRequest(ReqType.RESULT, append_result);
					m_output.writeObject(result);
					
				} // end append operation
				
				
				//////////////////
				// read operation
				//////////////////
				else if (ops.equalsIgnoreCase("r")) {
					int pos = Integer.parseInt(line.split("\\|")[2]);
					int length = Integer.parseInt(line.split("\\|")[3]);
					String display_msg = "";
					
					if (pos < 0 || length < 1) {
						System.out.println("Error: Read parameter error!");
						Thread.sleep(2000);
						continue;
					}
					
					m_output.writeObject(ReqType.READ);
					MetaRequest r_req = new MetaRequest(ReqType.READ, filename, pos, length);
					m_output.writeObject(r_req);
					
					ObjectInputStream m_input = new ObjectInputStream(m_s.getInputStream());
					MetaResponse meta_res = (MetaResponse) m_input.readObject();
					
					// check allocated file servers
					if (meta_res.file_server.size() == 0) {
						System.out.println("File not found according to meta-server, read skipped...");
						Thread.sleep(2000);
						continue;
					}
					
					// send read request to file server
					for (int i = 0; i < meta_res.file_server.size(); i++) {
						// pick a closet file server to read from
						String server = getClosestServer(meta_res.file_server.get(i));
						String blk = filename + "_" + (i+meta_res.start_blk_id);
						int start_pos = meta_res.pos[i];
						int read_len = meta_res.eff_length[i];
						
						OpsRequest req = new OpsRequest(ReqType.READ, blk, start_pos, read_len);
						
						boolean redo = false;  // flag indicating if file server operation succeeds or not
						int retry = 0;
						do {
							try {
								//System.out.println("Read " +blk+ "(" +start_pos+ ", "+read_len+
								//		") from file server " +server);
								// setup socket connection to file server
								Socket s = new Socket(server + ".utdallas.edu", 8822);
								ObjectOutputStream f_output = new ObjectOutputStream(s.getOutputStream());
								f_output.writeObject(req);
								
								ObjectInputStream f_input = new ObjectInputStream(s.getInputStream());
								String f_response = (String) f_input.readObject();
								
								if (f_response.isEmpty()) {
									redo = true;
								} else {
									display_msg += f_response;
									redo = false;
								}
							} catch (Exception ex) {
								//ex.printStackTrace();
								System.out.println("Error communicating with file server " + server);
								redo = true;
							}
							
							if (redo) {
								System.out.println("FAILED: " + "RETRY " + retry++);
								Thread.sleep(2000);
							}
            			
						} while (redo == true && retry < 3);
            			
						if (retry == 3) {
							System.out.println("Read FAILED!");
						} else {
							System.out.println(display_msg);
						}
					}
					
				} // end read operation
				
				Thread.sleep(2000);
			}
			br.close();
		} catch (IOException | InterruptedException | ClassNotFoundException e) {
			System.out.println("Exception: "+e.getMessage());
		}
	}
	
	
	// randomly pick a server as the closest server
	public static String getClosestServer(ArrayList<String> f_server) {
		return f_server.get((int)(f_server.size()*Math.random()));
	}
}
