import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;


@SuppressWarnings("serial")
class MetaRequest implements Serializable {
	ReqType type;
	String filename = "";  // filename to read/write/append
	int length = 0;        // length of the text for read/write/append
	int pos = 0;           // starting position for read
	boolean result = true; // write/append result. Flag for recording metadata
	
	MetaRequest(ReqType type, boolean result) {
		this.type = type;
		this.result = result;
	}
	
	MetaRequest(ReqType type, String filename, int length) {
		this.type = type;
		this.filename = filename;
		this.length = length;
	}
	
	MetaRequest(ReqType type, String filename, int pos, int length) {
		this.type = type;
		this.filename = filename;
		this.pos = pos;
		this.length = length;
	}
}


@SuppressWarnings("serial")
class MetaResponse implements Serializable {
	ArrayList<ArrayList<String>> file_server 
		= new ArrayList<ArrayList<String>>();  // file servers of index'ed block
	int[] eff_length = {};       			   // effective byte length of index'ed block
	int[] pos = {};              			   // starting position for read
	int start_blk_id = 0;       			   // offset of block index
	
	//boolean isNull = false;      // flag indicating if the last block should be
    							 // filled with \0 for append
}


//operation request
@SuppressWarnings("serial")
class OpsRequest implements Serializable {
	ReqType type;
	String filename = "";
	String block = "";
	String text = "";
	String[] to_replicate;
	int pos = 0;
	int read_length = 0;
	
	/*
	// PURGE request
	OpsRequest(ReqType type, String filename) {
		this.type = type;
		this.filename = filename;
	}
	*/
	
	// REPLICATE request
	OpsRequest(ReqType type, String block, String[] to_replicate) {
		this.type = type;
		this.block = block;
		this.to_replicate = to_replicate;
	}
	
	// CREATE/APPEND request
	OpsRequest(ReqType type, String block, String text, String[] to_replicate) {
		this.type = type;
		this.block = block;
		this.text = text;
		this.to_replicate = to_replicate;
	}
	
	// READ request
	OpsRequest(ReqType type, String block, int pos, int read_length) {
		this.type = type;
		this.block = block;
		this.pos = pos;
		this.read_length = read_length;
	}
}


@SuppressWarnings("serial")
class MetaData implements Serializable {
	int blk_id = 0;
	int eff_length = 0;
	String filename = "";
	
	// key: server contains a replica
	// value: server is valid of not
	Hashtable<String, DataNode> f_server = new Hashtable<String, DataNode>();
	
	// return true if given server is a replica of this block
	public boolean hasServer(String server) {
		for (DataNode node : f_server.values()) {
			if (node.name.equals(server)) return true;
		}
		return false;
	}
	
	// return all valid replica servers
	public ArrayList<String> getValidReplicas() {
		ArrayList<String> replicas = new ArrayList<String>();
		for (DataNode node : f_server.values()) {
			if (node.isValid == true) replicas.add(node.name);
		}
		return replicas;
	}
}


class HeartBeatMsg {
	ArrayList<MetaRequest> hb;
	DataNode node;
	
	HeartBeatMsg(ArrayList<MetaRequest> hb, DataNode node) {
		this.hb = hb;
		this.node = node;
	}
	
	@Override
	public String toString() {
		return "{" + hb.toString() + ", " + node.toString() + "}";
	}
}


// file server info
class DataNode {
	String name = "";
	int disk_size = 0;
	int space_left = 0;
	long update_time = 0;
	boolean isValid = true;
	
	DataNode() {}
	
	DataNode(String name, int size, int left) {
		this.name = name;
		this.disk_size = size;
		this.space_left = left;
	}
	
	@Override
	public String toString() {
		return name + "," + disk_size + "," + space_left;
	}
}


enum ReqType {
    READ, CREATE, APPEND,   // request initiated from client
    HEARTBEAT,              // request initiated from file server
    RESULT,                 // send from client, indicate if metadata is good to commit
    PROBE,                  // request initiated from file server asking for meta data
    //PURGE,                  // request initiated from meta server to purge meta data on file server
    REPLICATE,              // request initiated from meta server to replicate file block
}