package gash.router.server;

import org.slf4j.Logger;


import org.slf4j.LoggerFactory;

import gash.router.app.ServerApp;
import gash.router.client.CommInit;
import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeList;
import gash.router.server.raft.NodeState;
import gash.router.server.timer.NodeTimer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import pipe.common.Common;
import pipe.common.Common.Chunk;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
//import pipe.common.Common.Failure;
import pipe.common.Common.Node;
import pipe.common.Common.Request;
//import pipe.common.Common.Request.RequestType;
import pipe.common.Common.Response;
import pipe.common.Common.Response.Status;
import pipe.common.Common.TaskType;
import pipe.common.Common.WriteBody;
import pipe.common.Common.WriteResponse;
import routing.Pipe.CommandMessage;
import routing.Pipe.WorkStealingRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;


/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class QueueCommandHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("cmd");
	protected RoutingConf conf;
	protected static Queue<CommandMessage> leaderMessageQue;
	protected static Queue<CommandMessage> nonLeaderMessageQue;
	private static HashMap<String, List<CommandMessage>> map = new HashMap<>();
	private static EdgeList outbound = new EdgeList();
	private static Node client;
	
	//static ChannelFuture cf;
	static EventLoopGroup group = new NioEventLoopGroup();
	
	public QueueCommandHandler(RoutingConf conf, Queue<CommandMessage> leaderMessageQue, 
			Queue<CommandMessage> nonLeaderMessageQue) {
		if (conf != null) {
			this.conf = conf;
			QueueCommandHandler.leaderMessageQue = leaderMessageQue;
			QueueCommandHandler.nonLeaderMessageQue = nonLeaderMessageQue;
		}
	}

	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 * 
	 * @param msg
	 */
	
	public static void init(EdgeInfo ei)
	{
		logger.info("Trying to connect to host ! " + ei.getHost());
		try {
			CommInit si = new CommInit(false);
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioSocketChannel.class).handler(si);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);


			// Make the connection attempt.
			ChannelFuture cf = b.connect(ei.getHost(), ei.getPort()).syncUninterruptibly();

			
			// want to monitor the connection to the server s.t. if we loose the
			// connection, we can try to re-establish it.
			// ClientClosedListener ccl = new ClientClosedListener(this);
			// channel.channel().closeFuture().addListener(ccl);
			ei.setChannel(cf.channel());
			ei.setActive(true);

			System.out.println(cf.channel().localAddress() + " -> open: " + cf.channel().isOpen()
					+ ", write: " + cf.channel().isWritable() + ", reg: " + cf.channel().isRegistered());

		} catch (Throwable ex) {
			System.out.println("failed to initialize the client connection " + ex.toString());
			ex.printStackTrace();
		}

	}
	
	/*public void handleWSRRequest(CommandMessage msg, Channel channel){
		
		
		if(msg.hasRequest()){
			logger.info("... inside Message.Hasrequest() ... ");
			Request req = msg.getRequest();
			
			client = req.getClient();
			if(req.hasRequestType()){
				logger.info("... inside Message.HasrequestType() ... ");
				if(req.getRequestType().getNumber() == TaskType.WRITEFILE_VALUE){*/
					/*
					 * Handling Write Requests
					 */
					/*logger.info("... inside Message.getRequest() == WRITE_FILE... ");
					if(req.hasRwb()){
						logger.info("... inside request.HasRWB() ... ");
						WriteBody wb = req.getRwb();
						
						logger.info("number of chunks: " + wb.getNumOfChunks());
						String fileName = wb.getFilename();
						if(!map.containsKey(fileName)){
							ArrayList<CommandMessage> list = new ArrayList<>(wb.getNumOfChunks());
							list.add(wb.getChunk().getChunkId(), msg);
							map.put(fileName, list);
							NodeTimer timer = new NodeTimer();
							ChunkInspector chunkInspector = new ChunkInspector(fileName, wb.getNumOfChunks());
							Thread t = new Thread(chunkInspector);
							logger.info("Timeout scheduled for : " + (System.currentTimeMillis() - msg.getHeader().getTime() + 2000) * wb.getNumOfChunks());
							timer.schedule(t, (System.currentTimeMillis() - msg.getHeader().getTime() + 2000) * wb.getNumOfChunks());
						}
						else{
							map.get(fileName).add(wb.getChunk().getChunkId(), msg);
						}
					}
				}
			}
			System.out.println("Queue Command Handler : OH i got a file to write");

		}
		
		
		if(msg.hasWsr() == true){
			
			logger.info("... inside Message.HasWSR() ... ");
			logger.info("WSR request received");
			logger.info("Leader Queue size : " + leaderMessageQue.size());
			
			WorkStealingRequest request = msg.getWsr();
			
			request.getNodeState();
			logger.info("WSR received from : " + request.getNodeState());
			/*
			 * Send requests to Leader if leaderQueue is not empty !
			 */
			
			/*if(leaderMessageQue.size() > 0 && Integer.parseInt(request.getNodeState()) == (NodeState.LEADER)){
				logger.info("... inside leaderMessagequeSize() > 0 && node state leader... leader queue size" + leaderMessageQue.size());
				String host = request.getHost();
				int port = request.getPort();
				CommandMessage task = leaderMessageQue.poll();*/
				/*
				 * Create Connection to host and port and write task to the channel
				 */
				/*logger.info("Before init");
				init(host, port);
				logger.info("After Init");*/
				/*EventLoopGroup group = new NioEventLoopGroup();
				try {
					CommInit si = new CommInit(false);
					Bootstrap b = new Bootstrap();
					b.group(group).channel(NioSocketChannel.class).handler(si);
					b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
					b.option(ChannelOption.TCP_NODELAY, true);
					b.option(ChannelOption.SO_KEEPALIVE, true);


					// Make the connection attempt.
					chanl = b.connect(host, port).syncUninterruptibly();

					
					// want to monitor the connection to the server s.t. if we loose the
					// connection, we can try to re-establish it.
					// ClientClosedListener ccl = new ClientClosedListener(this);
					// channel.channel().closeFuture().addListener(ccl);

					System.out.println(chanl.channel().localAddress() + " -> open: " + chanl.channel().isOpen()
							+ ", write: " + chanl.channel().isWritable() + ", reg: " + chanl.channel().isRegistered());

				} catch (Throwable ex) {
					System.out.println("failed to initialize the client connection " + ex.toString());
					ex.printStackTrace();
				}
				logger.info("Before writing to channel ");
				cf.channel().writeAndFlush(task);
				if (cf.isDone() && cf.isSuccess()) {
					System.out.println("Msg sent succesfully:");
				}
			}
			else if(nonLeaderMessageQue.size() > 0){
				logger.info("... inside Non Leader Request ... non laeader queue size : " + nonLeaderMessageQue.size());
				String host = request.getHost();
				int port = request.getPort();
				CommandMessage task = nonLeaderMessageQue.poll();
				/*
				 * Create Connection to host and port and write task to the channel
				 */
				//init(host, port);
				/*EventLoopGroup group = new NioEventLoopGroup();
				try {
					CommInit si = new CommInit(false);
					Bootstrap b = new Bootstrap();
					b.group(group).channel(NioSocketChannel.class).handler(si);
					b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
					b.option(ChannelOption.TCP_NODELAY, true);
					b.option(ChannelOption.SO_KEEPALIVE, true);


					// Make the connection attempt.
					chanl = b.connect(host, port).syncUninterruptibly();

					
					// want to monitor the connection to the server s.t. if we loose the
					// connection, we can try to re-establish it.
					// ClientClosedListener ccl = new ClientClosedListener(this);
					// channel.channel().closeFuture().addListener(ccl);

					System.out.println(chanl.channel().localAddress() + " -> open: " + chanl.channel().isOpen()
							+ ", write: " + chanl.channel().isWritable() + ", reg: " + chanl.channel().isRegistered());

				} catch (Throwable ex) {
					System.out.println("failed to initialize the client connection " + ex.toString());
					ex.printStackTrace();
				}
				
				cf.channel().writeAndFlush(task);
				if (cf.isDone() && cf.isSuccess()) {
					System.out.println("Msg sent succesfully:");
				}
			}
			else{
				logger.info("Queues are empty ! NO Task !!");
			}
			
		}
		
		
	}*/
	
	public void handleMessage(CommandMessage msg, Channel channel) {
		
		/* For Write Requests :
		 * 
		 * 
		 * When QS receives a chunk from the client; it creates a key with filename
		 * and stores the chunk in its cache.
		 * It sets a timer for the first chunk request of each new file;
		 * After timeout; it checks if all the chunks have been received
		 * If true; push all the chunks to leaderQueue
		 * else create connection back to client and send acknowledgement for missing chunks
		 * and again set a timer !
		 */
		logger.info("Handling msg in handleMessage()");
		logger.info("WSR request is : " + msg.hasWsr());
		
		try{
			if(msg.hasRequest()){
				logger.info("... inside Message.Hasrequest() ... ");
				Request req = msg.getRequest();
				
				client = req.getClient();
				
				if(req.hasRequestType()){
					logger.info("... inside Message.HasrequestType() ... ");
					if(req.getRequestType().getNumber() == TaskType.WRITEFILE_VALUE){
						/*
						 * Handling Write Requests
						 */
						logger.info("... inside Message.getRequest() == WRITE_FILE... ");
						if(req.hasRwb()){
							logger.info("... inside request.HasRWB() ... ");
							WriteBody wb = req.getRwb();
							
							logger.info("number of chunks: " + wb.getNumOfChunks());
							String fileName = wb.getFilename();
							if(!map.containsKey(fileName)){
								ArrayList<CommandMessage> list = new ArrayList<>(wb.getNumOfChunks());
								list.add(wb.getChunk().getChunkId(), msg);
								map.put(fileName, list);
								NodeTimer timer = new NodeTimer();
								ChunkInspector chunkInspector = new ChunkInspector(fileName, wb.getNumOfChunks());
								Thread t = new Thread(chunkInspector);
								//logger.info("Timeout scheduled for : " +  * wb.getNumOfChunks());
								ServerUtils.setRequestTimeout(fileName,(System.currentTimeMillis() - msg.getHeader().getTime() + 2000));
								timer.schedule(t, ServerUtils.getRequestTimeout(fileName) * wb.getNumOfChunks());
							}
							else{
								map.get(fileName).add(wb.getChunk().getChunkId(), msg);
							}
						}
						System.out.println("Queue Command Handler : OH i got a file to write");
	
					}
				
			
					else if(req.getRequestType().getNumber() == TaskType.READFILE_VALUE){
								/*
								 * Handling Read Requests
								 */
						logger.info("... inside req.getRequest() == READ_FILE ... ");
						nonLeaderMessageQue.offer(msg);
					}
				}
			}
			
			else if(msg.hasAnr()){
					/*
					 * Handling Add Node requests
					 */
					logger.info("... inside Message.HasANR() node addtion... ");
					leaderMessageQue.offer(msg);
			}
			
			else if(msg.hasWsr() == true){
					
					logger.info("... inside Message.HasWSR() ... ");
					logger.info("WSR request received");
					logger.info("Leader Queue size : " + leaderMessageQue.size());
					
					WorkStealingRequest request = msg.getWsr();
					
					request.getNodeState();
					logger.info("WSR received from : " + request.getNodeState());
					/*
					 * Send requests to Leader if leaderQueue is not empty !
					 */
					
					if(leaderMessageQue.size() > 0 && Integer.parseInt(request.getNodeState()) == (NodeState.LEADER)){
						logger.info("... inside leaderMessagequeSize() > 0 && node state leader... leader queue size" + leaderMessageQue.size());
						String host = request.getHost();
						int port = request.getPort();
						int nodeId = msg.getHeader().getNodeId();
						
						CommandMessage task = leaderMessageQue.poll();
						/*
						 * Create Connection to host and port and write task to the channel
						 */
						
						if(!outbound.getMap().containsKey(nodeId)){
							logger.info("Before init");
							init(outbound.addNode(nodeId, host, port));
							logger.info("After Init");
							logger.info("Before writing to channel ");
							outbound.getMap().get(nodeId).getChannel().writeAndFlush(task);
						}
						
						else if(outbound.getMap().get(nodeId).isActive() && outbound.getMap().get(nodeId).getChannel() != null){
							logger.info("Before writing to channel ");
							ChannelFuture cf = outbound.getMap().get(nodeId).getChannel().writeAndFlush(task);
							if (cf.isDone() && cf.isSuccess()) {
								System.out.println("Work Stealing task sent succesfully to Leader:");
							}
						}
						
					}
					else if(nonLeaderMessageQue.size() > 0){
						logger.info("... inside Non Leader Request ... non laeader queue size : " + nonLeaderMessageQue.size());
						String host = request.getHost();
						int port = request.getPort();
						int nodeId = msg.getHeader().getNodeId();
						CommandMessage task = nonLeaderMessageQue.poll();
						/*
						 * Create Connection to host and port and write task to the channel
						 */
						if(!outbound.getMap().containsKey(nodeId)){
							logger.info("Before init");
							init(outbound.addNode(nodeId, host, port));
							logger.info("After Init");
							logger.info("Before writing to channel ");
							outbound.getMap().get(nodeId).getChannel().writeAndFlush(task);
						}
						
						else if(outbound.getMap().get(nodeId).isActive() && outbound.getMap().get(nodeId).getChannel() != null){
							logger.info("Before writing to channel ");
							ChannelFuture cf = outbound.getMap().get(nodeId).getChannel().writeAndFlush(task);
							if (cf.isDone() && cf.isSuccess()) {
								System.out.println("Work Stealing task sent succesfully to Node:");
							}
						}
			
					}
					else{
						logger.info("Queues are empty ! NO Task !!");
					}
					
				}
				else if(msg.hasResponse()){
					if(msg.getResponse().getResponseType().equals(TaskType.READFILE)){
						Response response = msg.getResponse();
						QueueCommandHandler.sendAcknowledgement(response);
					}
				}
			
			} catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(conf.getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}

		System.out.flush();
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		
		logger.info("Request arrived from : " + msg.getHeader().getNodeId());
		handleMessage(msg, ctx.channel());
		
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}
	
	private static class ChunkInspector implements Runnable{
		
		String fileName;
		int numberOfChunks;

		public ChunkInspector(String fileName, int numberOfChunks) {
			// TODO Auto-generated constructor stub
			this.fileName = fileName;
			this.numberOfChunks = numberOfChunks;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			
			ArrayList<CommandMessage> list = (ArrayList<CommandMessage>) getMapInstance().get(fileName);
			if(list.size() == numberOfChunks){
				/*
				 * Push the message to the leader Queue
				 */
				logger.info("Received all Chunks ");
				QueueCommandHandler.enqueue(fileName);
				
				/*
				 * Send writeResponse back to client
				 */
				Response.Builder response = Response.newBuilder();
				response.setFilename(fileName);
				response.setResponseType(TaskType.WRITEFILE);
				response.setStatus(Status.Success);
				Response resp = response.build();
				QueueCommandHandler.sendAcknowledgement(resp);
			}
			else{
				/*
				 * Check for missing chunks and create ack object and send back to client
				 */
				Response.Builder response = Response.newBuilder();
				WriteResponse.Builder wr = WriteResponse.newBuilder();
				ArrayList<Integer> chunkIds = new ArrayList<>();
				
				for(int i=0; i<list.size(); i++){
					if(list.get(i) == null){
						chunkIds.add(i);
						/*
						 * TODO : Need to add fileName as well
						 */
					}
				}
				wr.addAllChunkId(chunkIds);
				response.setFilename(fileName);
				response.setWriteResponse(wr);
				response.setResponseType(TaskType.WRITEFILE);
				response.setStatus(Status.Failure);
				Response resp = response.build();
				QueueCommandHandler.sendAcknowledgement(resp);
				
				
				NodeTimer timer = new NodeTimer();
				ChunkInspector chunkInspector = new ChunkInspector(fileName, numberOfChunks);
				Thread t = new Thread(chunkInspector);
				timer.schedule(t, ServerUtils.getRequestTimeout(fileName) * numberOfChunks);
				
				
			}
		}
		
	}
	
	public static void sendAcknowledgement(Response response){
		
		CommandMessage.Builder command = CommandMessage.newBuilder();
		Header.Builder header = Header.newBuilder();
		header.setNodeId(0);
		header.setTime(System.currentTimeMillis());
		command.setHeader(header);
		command.setResponse(response);
		ChannelFuture channel = null;
		EventLoopGroup group = new NioEventLoopGroup();
		try {
			CommInit si = new CommInit(false);
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioSocketChannel.class).handler(si);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);


			// Make the connection attempt.
			channel = b.connect(client.getHost(), client.getPort()).syncUninterruptibly();

			
			// want to monitor the connection to the server s.t. if we loose the
			// connection, we can try to re-establish it.
			// ClientClosedListener ccl = new ClientClosedListener(this);
			// channel.channel().closeFuture().addListener(ccl);

			System.out.println(channel.channel().localAddress() + " -> open: " + channel.channel().isOpen()
					+ ", write: " + channel.channel().isWritable() + ", reg: " + channel.channel().isRegistered());

		} catch (Throwable ex) {
			System.out.println("failed to initialize the client connection " + ex.toString());
			ex.printStackTrace();
		}
		CommandMessage cmd = command.build();
		channel.channel().writeAndFlush(cmd);
		if (channel.isDone() && channel.isSuccess()) {
			System.out.println("Msg sent succesfully:");
		}
		
	}
	
	public static void enqueue(String fileName){
		ArrayList<CommandMessage> list = (ArrayList<CommandMessage>) getMapInstance().get(fileName);
		for(CommandMessage msg : list){
			/*
			 * Build CommandMessage 
			 */
			leaderMessageQue.offer(msg);
			
		}
		logger.info("Added file chunks to leaderQueue");
	}
	
	public static HashMap<String, List<CommandMessage>> getMapInstance(){
		return map;
	}
	

}