import java.io.Serializable;

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
	String[] file_server = {};   // file server name of current index block
	int[] eff_length = {};       // effective byte length of index block
	
	boolean isNull = false;      // flag indicating if the last block should be
							     // filled with \0 for append
	int pos = 0;                 // starting position for append
	
}

enum ReqType {
    READ, CREATE, APPEND,   // request initiated from client
    HEARTBEAT,              // request initiated from file server
    RESULT,                 // send from client, indicate if metadata is good to commit
    PURGE                   // request initiated from meta server to file server
}

//operation request
@SuppressWarnings("serial")
class OpsRequest implements Serializable {
	ReqType type;
	String block = "";
	String text = "";
	int pos = 0;
	int read_length = 0;
	
	// PURGE request
	OpsRequest(ReqType type, String block) {
		this.type = type;
		this.block = block;
	}
	
	// CREATE/APPEND request
	OpsRequest(ReqType type, String block, String text) {
		this.type = type;
		this.block = block;
		this.text = text;
	}
	
	// READ request
	OpsRequest(ReqType type, String block, int pos, int read_length) {
		this.type = type;
		this.pos = pos;
		this.read_length = read_length;
	}
}