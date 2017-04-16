package gash.router.app;

import gash.router.client.MessageClient;
import gash.router.server.MessageServer;

import java.io.File;

import routing.Pipe.CommandMessage;

public class QueueServerApp {


	 private static int  id;
	//protected RingNode previous;
	//ip of previous server
		//protected static String next="169.254.67.22";
		protected static String next="169.254.121.22";
		//169.254.52.52
	//protected RingNode next;
	//ip of next server
	protected static String previous="169.254.85.237";

	private QueueServer server;
	private static MessageClient client;


	public QueueServerApp() {
		
		}
	public QueueServerApp(int id) {

		this.id=id;
		
		
		File cf = new File("runtime/queueServer.conf");
		try {
			server = new QueueServer(cf);
			server.startServer();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("server closing");
		}
	}
/*
	public static void propagateMessage(String msg) {
		//System.out.println("sendMessage(): " + msg.toString());
		
		//msg.incrementHops(nodeId);
		
			if (next != null){
				if(client==null){
				//call the client and forward the message next host and port
				client = new MessageClient(next, 4168);}
				System.out.println("I am here");
				client.ping();
				//next.message(msg);
				}
			else if (previous != null) {
				if(client==null){
					//call the client and forward the message next host and port
					client = new MessageClient(next, 4168);}

				client.ping();
			}
		
	}
	public static void propagateMessage(CommandMessage msg) {
		System.out.println("destination :"+msg.getHeader().getDestination());
		if(msg.getHeader().getDestination()==id){
			System.out.println("I got a message!");
			return;
		}
		
			if (next != null){
				if(client==null){
				//call the client and forward the message next host and port
				client = new MessageClient(next, 4168);}
				
				client.sendMessage(msg);
				//next.message(msg);
				}
			else if (previous != null) {
				if(client==null){
					//call the client and forward the message next host and port
					client = new MessageClient(next, 4168);}

				client.sendMessage(msg);
			}
		
	}


	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		//String host = "169.254.82.122";
		//int port = 4567;
		System.out.println("test");
		QueueServerApp app= new QueueServerApp(0);
		
	}
}
