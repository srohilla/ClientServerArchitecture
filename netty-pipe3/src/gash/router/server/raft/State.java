package gash.router.server.raft;


import pipe.common.Common.Request;
import pipe.common.Common.WriteBody;
import pipe.work.Work.WorkMessage;


/**
 * State pattern : Abstract class for leader , follower , candidate 
 * @author seemarohilla
 *
 */
public class State {
	
	

	protected volatile Boolean running = Boolean.TRUE;
	static Thread cthread;
	
	public void startService(State state) {

	}

	public void stopService() {
		// TODO Auto-generated method stub

	}

	public void handleResponseVoteRPCs(WorkMessage workMessage) {
		// TODO Auto-generated method stub

	}

	public WorkMessage handleRequestVoteRPC(WorkMessage workMessage) {
		// TODO Auto-generated method stub
		return null;
	}

	public void sendHeartBeat() {

	}

	public void handleHeartBeat(WorkMessage wm) {

	}
	
	public void handleHeartBeatResponse(WorkMessage wm) {

	}

	public void handleAppendEntries(WorkMessage wm) {

	}
	
	
	
	public byte[] handleGetMessage(String key) {
		return new byte[1];
	}
	
	public String handlePostMessage(byte[] image, long timestamp) {
		return null;
	}

	public void handlePutMessage(String key, byte[] image, long timestamp) {
		
	}
	
	public void handleDelete(String key) {
		
	}
	
	public void sendAppendEntries(WriteBody wm) {
		
	}
	

	public void handleWriteFile(WriteBody msg) {
		
	}


}
