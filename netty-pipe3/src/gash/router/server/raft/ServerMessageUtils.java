package gash.router.server.raft;

import com.google.protobuf.ByteString;

import gash.router.server.ServerUtils;
import pipe.work.AppendEntriesRPC;
import pipe.work.AppendEntriesRPC.AppendEntries;
import pipe.work.AppendEntriesRPC.AppendEntries.RequestType;
import pipe.work.AppendEntriesRPC.AppendEntriesPacket;
import pipe.work.AppendEntriesRPC.AppendEntriesResponse;
import pipe.work.AppendEntriesRPC.AppendEntriesResponse.IsUpdated;
import pipe.work.HeartBeatRPC.HeartBeat;
import pipe.work.HeartBeatRPC.HeartBeatPacket;
import pipe.work.HeartBeatRPC.HeartBeatResponse;
import pipe.work.VoteRPC.RequestVoteRPC;
import pipe.work.VoteRPC.ResponseVoteRPC;
import pipe.work.VoteRPC.ResponseVoteRPC.IsVoteGranted;
import pipe.work.VoteRPC.VoteRPCPacket;
import pipe.work.Work.WorkMessage;

public class ServerMessageUtils {

	public static WorkMessage prepareRequestVoteRPC() {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(ServerUtils.getCurrentUnixTimeStamp());

		RequestVoteRPC.Builder requestVoteRPC = RequestVoteRPC.newBuilder();
		requestVoteRPC.setTerm(NodeState.getInstance().getServerState().getConf().getNodeId());
		requestVoteRPC.setCandidateId("" + NodeState.getInstance().getServerState().getConf().getNodeId());
		requestVoteRPC.setTerm(NodeState.currentTerm);
		requestVoteRPC.setTimeStampOnLatestUpdate(NodeState.getTimeStampOnLatestUpdate());
		// requestVoteRPC.setTimeStampOnLatestUpdate(DatabaseService.getInstance().getDb().getCurrentTimeStamp());

		VoteRPCPacket.Builder voteRPCPacket = VoteRPCPacket.newBuilder();
		voteRPCPacket.setUnixTimestamp(ServerUtils.getCurrentUnixTimeStamp());
		voteRPCPacket.setRequestVoteRPC(requestVoteRPC);
		
		work.setVoteRPCPacket(voteRPCPacket);

		return work.build(); 
		
	}

	public static WorkMessage prepareAppendEntriesResponse() {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(ServerUtils.getCurrentUnixTimeStamp());

		AppendEntriesPacket.Builder appendEntriesPacket = AppendEntriesPacket.newBuilder();
		appendEntriesPacket.setUnixTimeStamp(ServerUtils.getCurrentUnixTimeStamp());

		AppendEntriesResponse.Builder appendEntriesResponse = AppendEntriesResponse.newBuilder();

		appendEntriesResponse.setIsUpdated(IsUpdated.YES);

		appendEntriesPacket.setAppendEntriesResponse(appendEntriesResponse);

		work.setAppendEntriesPacket(appendEntriesPacket);

		return work.build();

	}

	public static WorkMessage prepareHeartBeatResponse() {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(ServerUtils.getCurrentUnixTimeStamp());

		HeartBeatResponse.Builder heartbeatResponse = HeartBeatResponse.newBuilder();
		heartbeatResponse.setNodeId(NodeState.getInstance().getServerState().getConf().getNodeId());
		heartbeatResponse.setTerm(NodeState.currentTerm);
		heartbeatResponse.setTimeStampOnLatestUpdate(NodeState.getTimeStampOnLatestUpdate());
		// heartbeatResponse.setTimeStampOnLatestUpdate(DatabaseService.getInstance().getDb().getCurrentTimeStamp());
		HeartBeatPacket.Builder heartBeatPacket = HeartBeatPacket.newBuilder();
		heartBeatPacket.setUnixTimestamp(ServerUtils.getCurrentUnixTimeStamp());
		heartBeatPacket.setHeartBeatResponse(heartbeatResponse);
		
		work.setHeartBeatPacket(heartBeatPacket);

		return work.build();

	}

	public static WorkMessage prepareHeartBeat() {
		
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(ServerUtils.getCurrentUnixTimeStamp());

		HeartBeat.Builder heartbeat = HeartBeat.newBuilder();
		heartbeat.setLeaderId(NodeState.getInstance().getServerState().getConf().getNodeId());
		heartbeat.setTerm(NodeState.currentTerm);
		// Optional

		heartbeat.setTimeStampOnLatestUpdate(NodeState.getTimeStampOnLatestUpdate());

		// heartbeat.setTimeStampOnLatestUpdate(DatabaseService.getInstance().getDb().getCurrentTimeStamp());
		HeartBeatPacket.Builder heartBeatPacket = HeartBeatPacket.newBuilder();
		heartBeatPacket.setUnixTimestamp(ServerUtils.getCurrentUnixTimeStamp());
		heartBeatPacket.setHeartbeat(heartbeat);

		work.setHeartBeatPacket(heartBeatPacket);

		return work.build();
		
	}

	public static WorkMessage prepareAppendEntriesPacket(String key, byte[] imageData, long timestamp,
			RequestType type) {

		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(ServerUtils.getCurrentUnixTimeStamp());

		AppendEntriesPacket.Builder appendEntriesPacket = AppendEntriesPacket.newBuilder();
		appendEntriesPacket.setUnixTimeStamp(ServerUtils.getCurrentUnixTimeStamp());

		AppendEntriesRPC.ImageMsg.Builder imageMsg = AppendEntriesRPC.ImageMsg.newBuilder();
		imageMsg.setKey(key);

		ByteString byteString = null;
		if (imageData == null) {
			byteString = ByteString.copyFrom(new byte[1]);
		} else {
			byteString = ByteString.copyFrom(imageData);
		}
		imageMsg.setImageData(byteString);

		AppendEntries.Builder appendEntries = AppendEntries.newBuilder();
		appendEntries.setTimeStampOnLatestUpdate(timestamp);
		appendEntries.setImageMsg(imageMsg);
		appendEntries.setLeaderId(NodeState.getInstance().getServerState().getConf().getNodeId());

		appendEntries.setRequestType(type);
		appendEntriesPacket.setAppendEntries(appendEntries);

		work.setAppendEntriesPacket(appendEntriesPacket);

		return work.build();

	}

	public static WorkMessage prepareResponseVoteRPC(IsVoteGranted decision) {
		
		
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(ServerUtils.getCurrentUnixTimeStamp());

		VoteRPCPacket.Builder voteRPCPacket = VoteRPCPacket.newBuilder();
		voteRPCPacket.setUnixTimestamp(ServerUtils.getCurrentUnixTimeStamp());

		ResponseVoteRPC.Builder responseVoteRPC = ResponseVoteRPC.newBuilder();
		responseVoteRPC.setTerm(NodeState.getInstance().getServerState().getConf().getNodeId());
		responseVoteRPC.setIsVoteGranted(decision);

		voteRPCPacket.setResponseVoteRPC(responseVoteRPC);

		work.setVoteRPCPacket(voteRPCPacket);

		return work.build();
	}
	
	
}
