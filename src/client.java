import java.io.Serializable;



public class client {
	
	public static void main(String[] args) {
		
	}
}

@SuppressWarnings("serial")
class Request implements Serializable {
	REQTYPE type;
	
}

enum REQTYPE {
    READ, CREATE, APPEND
}