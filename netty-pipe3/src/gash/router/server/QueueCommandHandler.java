package gash.router.server;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import gash.router.app.ServerApp;
import gash.router.client.CommInit;
import gash.router.container.RoutingConf;
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
	private static Node client;
	
	
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
	public void handleMessage(CommandMessage msg, ChannelFuture channel) {
		
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
		if(msg.hasRequest()){
			Request req = msg.getRequest();
			client = req.getNode();
			if(req.hasRequestType()){
				if(req.getRequestType().getNumber() == TaskType.WRITEFILE_VALUE){
					/*
					 * Handling Write Requests
					 */
					if(req.hasRwb()){
						WriteBody wb = req.getRwb();
						String fileName = wb.getFilename();
						if(!map.containsKey(fileName)){
							map.put(fileName, new ArrayList<CommandMessage>(wb.getNumOfChunks()));
							NodeTimer timer = new NodeTimer();
							ChunkInspector chunkInspector = new ChunkInspector(fileName, wb.getNumOfChunks());
							Thread t = new Thread(chunkInspector);
							timer.schedule(t, ServerUtils.getFileReceiveTimeout());
						}
						else{
							map.get(fileName).add(wb.getChunk().getChunkId(), msg);
						}
					}
				}
				else if(req.getRequestType().getNumber() == TaskType.READFILE_VALUE){
					/*
					 * Handling Read Requests
					 */
					nonLeaderMessageQue.offer(msg);
				}
			}
			else if(msg.hasAnr()){
				/*
				 * Handling Add Node requests
				 */
				leaderMessageQue.offer(msg);
			}
			else if(msg.hasWsr()){
				/*
				 * Handling Work Stealing Requests
				 */
				WorkStealingRequest request = msg.getWsr();
				/*
				 * Send requests to Leader if leaderQueue is not empty !
				 */
				if(leaderMessageQue.size() > 0 && request.getNodeState().equals(NodeState.LEADER)){
					
					String host = request.getHost();
					int port = request.getPort();
					CommandMessage task = leaderMessageQue.poll();
					/*
					 * Create Connection to host and port and write task to the channel
					 */
					EventLoopGroup group = new NioEventLoopGroup();
					try {
						CommInit si = new CommInit(false);
						Bootstrap b = new Bootstrap();
						b.group(group).channel(NioSocketChannel.class).handler(si);
						b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
						b.option(ChannelOption.TCP_NODELAY, true);
						b.option(ChannelOption.SO_KEEPALIVE, true);


						// Make the connection attempt.
						 channel = b.connect(host, port).syncUninterruptibly();

						
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
					
					channel.channel().writeAndFlush(task);
					if (channel.isDone() && channel.isSuccess()) {
						System.out.println("Msg sent succesfully:");
					}
				}
				else{
					String host = request.getHost();
					int port = request.getPort();
					CommandMessage task = nonLeaderMessageQue.poll();
					/*
					 * Create Connection to host and port and write task to the channel
					 */
					EventLoopGroup group = new NioEventLoopGroup();
					try {
						CommInit si = new CommInit(false);
						Bootstrap b = new Bootstrap();
						b.group(group).channel(NioSocketChannel.class).handler(si);
						b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
						b.option(ChannelOption.TCP_NODELAY, true);
						b.option(ChannelOption.SO_KEEPALIVE, true);


						// Make the connection attempt.
						 channel = b.connect(host, port).syncUninterruptibly();

						
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
					
					channel.channel().writeAndFlush(task);
					if (channel.isDone() && channel.isSuccess()) {
						System.out.println("Msg sent succesfully:");
					}
				}
				
			}
			else if(msg.hasResponse()){
				if(msg.getResponse().getResponseType().equals(TaskType.READFILE)){
					Response response = msg.getResponse();
					QueueCommandHandler.sendAcknowledgement(response);
				}
			}
		}
		
		/* Code to schedule a timer !!
		 * timer = new NodeTimer();
			timer.schedule(new Runnable() {
				@Override
				public void run() {
	
					
				}
			}, ServerUtils.getFixedTimeout());
		 */
		
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}
       // ServerApp.propagateMessage(msg);
		//PrintUtil.printCommand(msg);
         
		try {
			// TODO How can you implement this without if-else statements?
			if (msg.hasPing()) {
				
				logger.info("ping from " + msg.getHeader().getNodeId());
				//to distribute this message internally
			} else if (msg.hasMessage()) {
				logger.info(msg.getMessage());
			}
			else if (msg.hasRequest() == true) {
				System.out.println("OH i got a file to write");
				NodeState.getInstance().getState().handleWriteFile(msg.getRequest().getRwb());
				
			}
			else {
				//TODO
				
			}

		} catch (Exception e) {
			// TODO add logging
			/*Failure.Builder eb = Failure.newBuilder();
			eb.setId(conf.getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());*/
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
		handleMessage(msg, (ChannelFuture) ctx.channel());
		/*System.out.println(" Pushing haschode to messageQue");

		// if it is a write message
		if(msg.getRequest().getRequestType().getNumber() == RequestType.WRITEFILE_VALUE)
			leaderMessageQue.add(msg);
		
		else
			nonLeaderMessageQue.add(msg);
			*/
		
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
				QueueCommandHandler.enqueue(fileName);
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
				ChunkInspector chunkInspector = new ChunkInspector(fileName, numberOfChunks );
				Thread t = new Thread(chunkInspector);
				timer.schedule(t, ServerUtils.getFileReceiveTimeout());
				
			}
		}
		
	}
	
	public static void sendAcknowledgement(Response response){
		CommandMessage.Builder command = CommandMessage.newBuilder();
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
		
		((Channel) channel).writeAndFlush(command);
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
	}
	
	public static HashMap<String, List<CommandMessage>> getMapInstance(){
		return map;
	}
	

}