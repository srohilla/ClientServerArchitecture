package gash.router.server.edges;

/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */


import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import gash.router.client.CommInit;
import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.logger.Logger;
import gash.router.server.ServerState;
import gash.router.server.ServerUtils;
import gash.router.server.WorkHandler;
import gash.router.server.WorkInit;
import gash.router.server.raft.NodeState;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import pipe.common.Common.Header;
import pipe.common.Common.Node;
import pipe.work.Ping.PingMessage;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;
import routing.Pipe.WorkStealingRequest;

public class EdgeMonitor implements EdgeListener, Runnable {

	public boolean isWorkAvailable() {
		return isWorkAvailable;
	}

	public void setWorkAvailable(boolean isWorkAvailable) {
		this.isWorkAvailable = isWorkAvailable;
	}

	public EdgeList getOutboundEdges() {
		return outboundEdges;
	}

	public EdgeList getInboundEdges() {
		return inboundEdges;
	}

	private EdgeList outboundEdges;
	private EdgeList inboundEdges;
	private long dt = 2000;
	private ServerState state;
	private boolean forever = true;
	private int port; 
	private boolean isWorkAvailable = false;

	public EdgeMonitor(ServerState state) {
		port = state.getConf().getCommandPort();
		Logger.DEBUG("Port : "+ port);
		if (state == null)
			throw new RuntimeException("state is null");

		this.outboundEdges = new EdgeList();
		this.inboundEdges = new EdgeList();
		this.state = state;
		this.state.setEmon(this);

		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				EdgeInfo ei = outboundEdges.addNode(e.getId(), e.getHost(), e.getPort());
			}
		}
		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();
	}

	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
	}

	private WorkMessage createPingMessage(EdgeInfo ei) {

		WorkMessage.Builder work = WorkMessage.newBuilder();
		PingMessage.Builder pingMessage = PingMessage.newBuilder();

		pingMessage.setNodeId(state.getConf().getNodeId());
		try {
			pingMessage.setIP(InetAddress.getLocalHost().getHostAddress());
			pingMessage.setPort(0000);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		work.setUnixTimeStamp(ServerUtils.getCurrentUnixTimeStamp());
		work.setTrivialPing(pingMessage);

		return work.build();
	}

	public void shutdown() {
		forever = false;
	}

	@Override
	public void run() {
		while (forever) {
			try {
				for (EdgeInfo ei : this.outboundEdges.map.values()) {
					if (ei.isActive() && ei.getChannel() != null) {
						//if(state != null && state.getTasks() != null){
							if(ei.getRef() == 0 && (state.getTasks() == null || state.getTasks().numEnqueued() == 0)){ //Value 0 is for proxy server
								Logger.DEBUG("Sending WSR to Proxy ");
								CommandMessage.Builder cmd = CommandMessage.newBuilder();
								WorkStealingRequest.Builder wsr = WorkStealingRequest.newBuilder();
								wsr.setHost(state.getConf().getHost());
								wsr.setPort(port); //Command Port
								wsr.setNodeState(String.valueOf(NodeState.getInstance().getNodestate()));
								Logger.DEBUG("WSR request before set : " + wsr);
								cmd.setWsr(wsr);
								Logger.DEBUG("WSR request after set " + cmd.getWsr());
								Header.Builder header= Header.newBuilder();
								header.setNodeId(state.getConf().getNodeId());
								header.setTime(999);
								cmd.setHeader(header);
								CommandMessage commandMessage = cmd.build();
								ChannelFuture cf = ei.getChannel().writeAndFlush(commandMessage);
								//Thread.sleep(4000);
								if (cf.isDone() && cf.isSuccess()) {
									Logger.DEBUG("Message sent to proxy !");
								}
							}
						
						else if(state == null){
							Logger.DEBUG("State is null");
						}
						else{
							Logger.DEBUG("Tasks is null");
						}
						
					}else {
						Logger.DEBUG("Chanel not active for : " + ei.getRef());
						if(ei.getRef() == 0){
							/*
							 * Make connection with QueueServer
							 */
							connectToQueueServer(ei);
						}
						else{
							onAdd(ei);
						}

						// Logger.DEBUG("Connection made 1234" +
						// ei.getChannel().config().hashCode());
					}
				}

				Thread.sleep(dt);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
				try {
					throw e;
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}
	
	public synchronized void connectToQueueServer(EdgeInfo ei){
		
		EventLoopGroup group = new NioEventLoopGroup();
		ChannelFuture channel; 
		try {
			CommInit si = new CommInit(false);
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioSocketChannel.class).handler(si);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);


			// Make the connection attempt.
			Logger.DEBUG("Host : " + ei.getHost() + "Port "+ ei.getPort());
			channel = b.connect(ei.getHost(), ei.getPort()).syncUninterruptibly();

			
			// want to monitor the connection to the server s.t. if we loose the
			// connection, we can try to re-establish it.
			// ClientClosedListener ccl = new ClientClosedListener(this);
			// channel.channel().closeFuture().addListener(ccl);

			System.out.println(channel.channel().localAddress() + " -> open: " + channel.channel().isOpen()
					+ ", write: " + channel.channel().isWritable() + ", reg: " + channel.channel().isRegistered());
			
			ei.setChannel(channel.channel());
			ei.setActive(true);
			//channel.channel().closeFuture();
			
		} catch (Throwable ex) {
			System.out.println("failed to initialize the client connection " + ex.toString());
			ex.printStackTrace();
		}
		
	}
	
	@Override
	public synchronized void onAdd(EdgeInfo ei) {
		try {
			
			EventLoopGroup group = new NioEventLoopGroup();
			Bootstrap b = new Bootstrap();
			b.handler(new WorkHandler(state));

			b.group(group).channel(NioSocketChannel.class).handler(new WorkInit(state, false));
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			// Make the connection attempt.
			ChannelFuture cf = b.connect(ei.getHost(), ei.getPort()).syncUninterruptibly();

			ei.setChannel(cf.channel());
			ei.setActive(true);
			cf.channel().closeFuture();
		} catch (Exception ex) {
		//	ex.printStackTrace();
			// Vinit --- > Nullifying the exception so that it further sends the
			// PingMessage
		}

	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		
	}
}
