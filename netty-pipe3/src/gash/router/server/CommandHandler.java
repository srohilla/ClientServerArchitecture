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
package gash.router.server;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import gash.router.app.ServerApp;
import gash.router.container.RoutingConf;
import gash.router.server.raft.NodeState;
import gash.router.server.tasks.TaskList;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
//import pipe.common.Common.Failure;
//import pipe.common.Common.Request.RequestType;
import routing.Pipe.CommandMessage;


/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class CommandHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("cmd");
	protected RoutingConf conf;
	protected ServerState state;
	
	public CommandHandler(RoutingConf conf, ServerState state) {
		if (conf != null) {
			this.conf = conf;
		}
		if(state != null){
			this.state = state;
		}
	}

	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 * 
	 * @param msg
	 */
	public void handleMessage(CommandMessage msg, Channel channel) {
		
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
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(conf.getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}
		
		state.getTasks().dequeue();
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
		//ServerState state = NodeState.getInstance().getServerState();
		if(state.getTasks() != null){
			state.getTasks().addTask(msg);
		}
		else{
			TaskList task = new TaskList();
			task.addTask(msg);
			state.setTasks(task);
		}
		
		//NodeState.getInstance().getServerState().setTasks(task);
		//NodeState.getInstance().getServerState().getTasks().addTask(msg);
		logger.info("Message received by worker(leader/follower) to process");
		handleMessage(msg, ctx.channel());	
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}