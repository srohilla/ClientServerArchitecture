package gash.router.app;


import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.protobuf.ByteString;

import gash.router.client.CommInit;
import gash.router.server.ServerUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import pipe.common.Common.Chunk;
import pipe.common.Common.Header;
import pipe.common.Common.Request;
import pipe.common.Common.Request.RequestType;
import pipe.common.Common.WriteBody;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;


//import pipe.common.Common.Body.BodyType;



public class WriteClient {
	
	static String host;
	static int port;
	static ChannelFuture channel;
	
	 static EventLoopGroup group;
	

	
	public static void init(String host_received, int port_received)
	{
		host = host_received;
		port = port_received;
		group = new NioEventLoopGroup();
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

	}

	
	
	
	public static void writeFile(String filepath){
		
		try {

			Path path = Paths.get(filepath);
			byte[] data;
			data = Files.readAllBytes(path);
			ByteString byteStringData=ByteString.copyFrom(data);
		//	WorkMessage.Builder work = WorkMessage.newBuilder();
			CommandMessage.Builder command = CommandMessage.newBuilder();
		    Request.Builder msg = Request.newBuilder();
			msg.setRequestType(RequestType.WRITEFILE);
			WriteBody.Builder rwb  = WriteBody.newBuilder();
			rwb.setFileId("1");
			rwb.setFileExt("txt");
			rwb.setFilename("test1");
			rwb.setNumOfChunks(1);
			Header.Builder header= Header.newBuilder();
			header.setNodeId(1);
			command.setHeader(header);
			Chunk.Builder chunk = Chunk.newBuilder();
			chunk.setChunkId(1);
			chunk.setChunkSize(12);
			chunk.setChunkData(byteStringData);
			rwb.setChunk(chunk);
			msg.setRwb(rwb);
			command.setRequest(msg);
			
		
			
		//	work.setUnixTimeStamp(ServerUtils.getCurrentUnixTimeStamp());
		//	WorkMessage workMessage= work.build();
			CommandMessage commandMessage = command.build();
			
			channel.channel().writeAndFlush(commandMessage);
			
			if (channel.isDone() && channel.isSuccess()) {
			System.out.println("Msg sent succesfully:");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	

	public static void main(String[] args) {
		
		String host = "127.0.0.1";
		int port = 4168;
		
		System.out.println("Sent the message");
		
		WriteClient.init(host, port);
		WriteClient.writeFile("/Users/seemarohilla/Desktop/test.txt");
		//AdapterClientAPI.post("vinit_adapter".getBytes());;
	
		while(true){
			
		}
		// TODO Auto-generated method stub
		
	}

}