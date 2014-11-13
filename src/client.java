import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;



public class client {
	
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("\tUsage: client input_file");
			System.exit(2);
		}
		
		String input_file = args[0];
		BufferedReader br;
		try {
			String m_server = "dc30";  //hardcode dc30 as metadata server
			Socket m_s = new Socket(m_server + ".utdallas.edu", 8821);  // socket connection to metadata server
			ObjectInputStream m_input = new ObjectInputStream(m_s.getInputStream());
			ObjectOutputStream m_output = new ObjectOutputStream(m_s.getOutputStream());
			
			br = new BufferedReader(new FileReader(input_file));
			String line;
			// input file readline
			while ((line = br.readLine()) != null) {
				String ops = line.split("\\|")[0];
				String filename = line.split("\\|")[1];
				System.out.println("Ops: " +ops+ " " +filename);
				
				///////////////////
				// write operation
				///////////////////
				if (ops.equalsIgnoreCase("w")) {
					String text = line.split("\\|")[2];  // text to byte array
					
					MetaRequest w_req = new MetaRequest(ReqType.CREATE, filename, text.length());
					m_output.writeObject(w_req);
					
					while (m_input.available() == 0) {}  // wait for available input stream
					MetaResponse meta_res = (MetaResponse) m_input.readObject();
					
					// check allocated file servers
					if (meta_res.file_server.length == 0) {
						System.out.println("No file server available according to meta-server, operation skipped...");
						Thread.sleep(2000);
						continue;
					}
					
					// send write request to file server
					boolean write_result = true;
					for (int i = 0; i < meta_res.file_server.length; i++) {
						String server = meta_res.file_server[i];
						String blk = filename + "_" + i;
						String sub_text = text.substring(i*8192, i*8192+meta_res.eff_length[i]);
						
						OpsRequest req = new OpsRequest(ReqType.CREATE, blk, sub_text);
						
						boolean redo = false;  // flag indicating if file server operation succeeds or not
						int retry = 0;
						{
							try {
								System.out.println("Write " +blk+ "(" +meta_res.eff_length[i]+ 
										") to file server " +server);
								// setup socket connection to file server
								Socket s = new Socket(server + ".utdallas.edu", 8822);
								DataInputStream f_input = new DataInputStream(s.getInputStream());
								ObjectOutputStream f_output = new ObjectOutputStream(s.getOutputStream());
	                			
								f_output.writeObject(req);
								while (f_input.available() == 0) {}
								char f_response = f_input.readChar();
								
								if (f_response == 'n') {
									redo = true;
								} else {
									redo = false;
								}
							} catch (Exception ex) {
								ex.printStackTrace();
								System.out.println("Error communicating with file server " + server);
								redo = true;
							}
							
							if (redo) System.out.println("FAILED: " + "RETRY " + retry++);
            			
						} while (redo == true && retry < 3);
            			
						if (retry == 3) {
							System.out.println("Write " +blk+ "(" +meta_res.eff_length[i]+ 
									") to file server " +server+ " FAILED!");
							write_result = false;
						}
					}
					
					// reply write result back to meta server
					MetaRequest result = new MetaRequest(ReqType.RESULT, write_result);
					m_output.writeObject(result);
					
				}
				
				////////////////////
				// append operation
				////////////////////
				else if (ops.equalsIgnoreCase("a")) {
					byte[] text = line.split("\\|")[2].getBytes();
					
					MetaRequest a_req = new MetaRequest(ReqType.APPEND, filename, text.length);
					m_output.writeObject(a_req);
					
					while (m_input.available() == 0) {}  // wait for available input stream
					MetaResponse meta_res = (MetaResponse) m_input.readObject();
					
					// check allocated file servers
					if (meta_res.file_server.length == 0) {
						System.out.println("File not found or no file server available according to meta-server, operation skipped...");
						Thread.sleep(2000);
						continue;
					}
					
					
					
				}
				
				//////////////////
				// read operation
				//////////////////
				else if (ops.equalsIgnoreCase("r")) {
					int pos = Integer.parseInt(line.split("\\|")[2]);
					int length = Integer.parseInt(line.split("\\|")[3]);
					
					
				}
				
				Thread.sleep(2000);
			}
			br.close();
		} catch (IOException | InterruptedException | ClassNotFoundException e) {
			System.out.println("Exception: "+e.getMessage());
		}
	}
}
