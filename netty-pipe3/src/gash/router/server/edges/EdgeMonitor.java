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

import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.logger.Logger;
import gash.router.server.ServerState;
import gash.router.server.ServerUtils;
import gash.router.server.WorkHandler;
import gash.router.server.WorkInit;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import pipe.work.Ping.PingMessage;
import pipe.work.Work.WorkMessage;

public class EdgeMonitor implements EdgeListener, Runnable {

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

	public EdgeMonitor(ServerState state) {
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
					//	WorkMessage workMessage = createPingMessage(ei);
					//	Logger.DEBUG("Sent Ping Message to " + ei.getRef());
						//ChannelFuture cf = ei.getChannel().writeAndFlush(workMessage);
					//	if (cf.isDone() && !cf.isSuccess()) {
						//	Logger.DEBUG("failed to send Ping Message  to server");
					//	}
					} else {
						Logger.DEBUG("Chanel not active for : " + ei.getRef());
						onAdd(ei);

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
