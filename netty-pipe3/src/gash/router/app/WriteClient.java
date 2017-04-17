
package gash.router.app;


import java.io.BufferedInputStream;






import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.protobuf.ByteString;

import gash.router.client.CommInit;
import gash.router.logger.Logger;
import gash.router.server.ServerUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import pipe.common.Common.Chunk;
import pipe.common.Common.Header;
import pipe.common.Common.Node;
import pipe.common.Common.Request;
import pipe.common.Common.TaskType;
//import pipe.common.Common.Request.RequestType;
import pipe.common.Common.WriteBody;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;


//import pipe.common.Common.Body.BodyType;



public class WriteClient {
	
	static String host;
	static int port;
	static ChannelFuture channel;
	
	static EventLoopGroup group;
	static final int chunkSize = 1024; // MAX BLOB STORAGE = Math,pow(2,15) -1 = 65535 Bytes 
	
	
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

	
	
	
	public static void writeFile(File f){
		
			int partCounter = 0;
			byte[] data;
			Logger.DEBUG("File Size : " + f.length());
			long fileSize = f.length();
			int chunks = (int) Math.ceil(f.length()/(double)chunkSize);
			
			if(chunks == 0){
				data = new byte[(int) f.length()];
			}
			else{
				data = new byte[chunkSize];
			}
			
			try (BufferedInputStream bis = new BufferedInputStream(
			new FileInputStream(f))) {//try-with-resources to ensure closing stream
				
			String name = f.getName();
			//int tmp = chunks;
			while(fileSize > 0 && bis.read(data) > 0){
				
				Logger.DEBUG("File LEft : " + fileSize);
				if(fileSize < chunkSize){
					data = new byte[(int) fileSize];
					fileSize = 0;
				}

				fileSize -= chunkSize;
				
				ByteString byteStringData = ByteString.copyFrom(data);
				Logger.DEBUG(byteStringData.toStringUtf8());
				CommandMessage.Builder command = CommandMessage.newBuilder();
				Request.Builder msg = Request.newBuilder();
				msg.setRequestType(TaskType.WRITEFILE);
				WriteBody.Builder rwb  = WriteBody.newBuilder();
				rwb.setFileExt(f.getName().substring(f.getName().lastIndexOf(".") + 1));
				rwb.setFilename(f.getName());
				rwb.setNumOfChunks(chunks);
				Header.Builder header= Header.newBuilder();
				header.setNodeId(1);
				header.setTime(0);
				command.setHeader(header);
				Chunk.Builder chunk = Chunk.newBuilder();
				chunk.setChunkId(partCounter);
				chunk.setChunkSize(chunkSize);
				chunk.setChunkData(byteStringData);
				rwb.setChunk(chunk);
				msg.setRwb(rwb);
				Node.Builder node = Node.newBuilder();
				node.setHost(InetAddress.getLocalHost().getHostAddress());
				node.setPort(7777);
				node.setNodeId(-1);
				msg.setClient(node);
				command.setRequest(msg);
				partCounter++;
				CommandMessage commandMessage = command.build();
				
				channel.channel().writeAndFlush(commandMessage);
				
				if (channel.isDone() && channel.isSuccess()) {
					System.out.println("Msg sent succesfully:");
				}
			}
				
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	

	public static void main(String[] args) {
		/*long f = 2297;
		int chunkSize = 1024;
		System.out.println((int) Math.ceil(f/(double)chunkSize));*/
		String host = "127.0.0.1";
		int port = 4068;
		
		System.out.println("Sent the message");
		
		WriteClient.init(host, port);
		File file = new File("/Users/gaganjain/Desktop/DemoApp.java");
		WriteClient.writeFile(file);
		//AdapterClientAPI.post("vinit_adapter".getBytes());;
	
		while(true){
			
		}
		// TODO Auto-generated method stub
		
	}

}
