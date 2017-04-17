package gash.router.server.raft;

import gash.router.logger.Logger;
import gash.router.server.ServerUtils;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.timer.NodeTimer;
import io.netty.channel.ChannelFuture;
import pipe.work.Work.WorkMessage;
import pipe.work.VoteRPC.ResponseVoteRPC;



public class CandidateState extends State implements Runnable{

	private static CandidateState INSTANCE = null;
	private int numberOfYESResponses;
	private int TotalResponses;
	NodeTimer timer = new NodeTimer();

	private CandidateState() {
		// TODO Auto-generated constructor stub
	}

	public static CandidateState getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new CandidateState();
		}
		return INSTANCE;
	}

	@Override
	public void run() {
		
		Logger.DEBUG("-----------------------CANDIDATE SERVICE STARTED ----------------------------");
		startElection();
		while (running) {

		}
	}

	private void startElection() {
		numberOfYESResponses = 0;
		TotalResponses = 0;
		NodeState.currentTerm++;
		
		for (EdgeInfo ei : NodeState.getInstance().getServerState().getEmon().getOutboundEdges().getMap().values()) {
			Logger.DEBUG("I have started contacting");
			System.out.println(ei.isActive());
			System.out.println(ei.getChannel());
			
			if (ei.isActive() && ei.getChannel() != null && ei.getRef() != 0) {
				WorkMessage workMessage = ServerMessageUtils.prepareRequestVoteRPC();
				Logger.DEBUG("Sent VoteRequestRPC to " + ei.getRef());
				ChannelFuture cf = ei.getChannel().writeAndFlush(workMessage);
				if (cf.isDone() && !cf.isSuccess()) {
					Logger.DEBUG("failed to send message (VoteRequestRPC) to server");
				}
			}
		}
		timer = new NodeTimer();
		timer.schedule(new Runnable() {
			@Override
			public void run() {

				if (isWinner()) {
					Logger.DEBUG(NodeState.getInstance().getServerState().getConf().getNodeId() + " has won the election.");
					NodeState.getInstance().setNodeState(NodeState.LEADER);
				} else {
					Logger.DEBUG(NodeState.getInstance().getServerState().getConf().getNodeId() + " has lost the election.");
					NodeState.getInstance().setNodeState(NodeState.FOLLOWER);
				}
			}

			private Boolean isWinner() {

				Logger.DEBUG("Total number of responses = "+TotalResponses);
				Logger.DEBUG("Total number of YES responses = "+ numberOfYESResponses);
				
				if ((numberOfYESResponses + 1) > (TotalResponses + 1) / 2) {
					return Boolean.TRUE;
				}
				return Boolean.FALSE;

			}
		}, ServerUtils.getFixedTimeout());

	}

	@Override
	public void handleResponseVoteRPCs(WorkMessage workMessage) {
		TotalResponses++;
		
		if (workMessage.getVoteRPCPacket().getResponseVoteRPC()
				.getIsVoteGranted() == ResponseVoteRPC.IsVoteGranted.YES) {
			
			Logger.DEBUG("Vote 'YES' is granted from Node Id " + workMessage.getVoteRPCPacket().getResponseVoteRPC().getTerm());
			numberOfYESResponses++;
			
		}else{
			Logger.DEBUG("Vote 'NO' is granted from Node Id " + workMessage.getVoteRPCPacket().getResponseVoteRPC().getTerm());
		}
		

	}

	@Override
	public WorkMessage handleRequestVoteRPC(WorkMessage workMessage) {
		if (workMessage.getVoteRPCPacket().getRequestVoteRPC().getTimeStampOnLatestUpdate() < NodeState.getTimeStampOnLatestUpdate()) {
			return ServerMessageUtils.prepareResponseVoteRPC(pipe.work.VoteRPC.ResponseVoteRPC.IsVoteGranted.NO);

		}
		return ServerMessageUtils.prepareResponseVoteRPC(ResponseVoteRPC.IsVoteGranted.YES);
	}

	@Override
	public void handleHeartBeat(WorkMessage wm) {
		Logger.DEBUG("HeartbeatPacket received from leader :" + wm.getHeartBeatPacket().getHeartbeat().getLeaderId());

		NodeState.getInstance().setNodeState(NodeState.FOLLOWER);

	}

	@Override
	public void startService(State state) {

		running = Boolean.TRUE;
		cthread = new Thread((CandidateState) state);
		cthread.start();

	}

	public void stopService() {
		timer.cancel();
		running = Boolean.FALSE;

	}
	
	
	
	
	
	


}
