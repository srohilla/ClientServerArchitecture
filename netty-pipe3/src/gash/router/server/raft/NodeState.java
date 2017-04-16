package gash.router.server.raft;

import gash.router.database.DatabaseService;
import gash.router.logger.Logger;
import gash.router.server.ServerState;



public class NodeState {

	public static final int LEADER = 0;

	public static final int CANDIDATE = 1;

	public static final int FOLLOWER = 2;

	private static int nodestate = 2;
	
	public static int currentTerm = 0;

	private static long timeStampOnLatestUpdate = 0;
	
	private static long noTaskProcessed = 0;
	
	private static State state;

	private static NodeState instance = null;
	
	private  ServerState serverState = null;

	public static int getNodestate() {
		return nodestate;
	}
	public static NodeState getInstance() {
		if (instance == null) {
			instance = new NodeState();
		}
		return instance;
	}
	 /**
     * Setting the new state of the Node : Follower , Leader , Candidate
     * @param newState
     */
	public static void setNodeState(int newState) {
		
		nodestate = newState;
		//set the state based on value of newState
		
		if (newState == NodeState.FOLLOWER) {
			//if the new state recieved within that time is again Follower
			//than reset the follower state
			if(state!=null){
				state.stopService();
			}
			
			state = FollowerState.getInstance();
			state.startService(state);
		} else if (newState == NodeState.LEADER) {
			//if the new state recieved within that time is Leader
			//than start the leader service
	
			if(state!=null){
				state.stopService();
			}
			state = LeaderState.getInstance();
			state.startService(state);

		} else if (newState == NodeState.CANDIDATE) {
			//if the new state recieved within that time is Candidate
			//than start the candidate service
			if(state!=null){
				state.stopService();
			}
			state = CandidateState.getInstance();
			Logger.DEBUG("starting candidate");
			state.startService(state);
		}
	}

	public static int getCurrentTerm() {
		return currentTerm;
	}

	public static void updateTaskCount() {
		noTaskProcessed++;
	}
		
	public static State getState() {
		return state;
	}

	public static void setState(State state) {
		NodeState.state = state;
	}

	public static void setCurrentTerm(int currentTerm) {
		NodeState.currentTerm = currentTerm;
	}

	public static long getTimeStampOnLatestUpdate() {
		if (timeStampOnLatestUpdate == 0) {
			//get latest timestamp from the database
			//timeStampOnLatestUpdate = DatabaseService.getInstance().getDb().getCurrentTimeStamp();
		}
		return timeStampOnLatestUpdate;
	}

	public static void setTimeStampOnLatestUpdate(long timeStampOnLatestUpdate) {
		NodeState.timeStampOnLatestUpdate = timeStampOnLatestUpdate;
	}

	public static long getNoTaskProcessed() {
		return noTaskProcessed;
	}

	public static void setNoTaskProcessed(long noTaskProcessed) {
		NodeState.noTaskProcessed = noTaskProcessed;
	}
	
	public void setServerState(ServerState serverState){
		this.serverState= serverState;
	}
	
	public ServerState getServerState()
	{
		return serverState;
		
	}
	
	
	
	
	
	
}
