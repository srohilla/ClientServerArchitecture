package gash.router.app;

import java.io.File;

import gash.router.client.MessageClient;
import gash.router.server.MessageServer;

public class ServerApp {

 private static int  id;
//protected RingNode previous;
//ip of previous server
	//protected static String next="169.254.67.22";
	protected static String next="169.254.67.23";
//protected RingNode next;
//ip of next server
protected static String previous="169.254.112.208";
//my ip 169.254.181.93
private MessageServer server;
private static MessageClient client;


public ServerApp() {
	
	}
public ServerApp(int id) {
//	super(id);
	this.id=id;
	//client = new MessageClient(next, 4567);
//	MessageClient mc= new MessageClient(next,4567);
	//mc.ping();
//	mc.release();
	
	File cf = new File("resources/routing.conf");
	try {
		server = new MessageServer(cf);
		server.startServer();
	} catch (Exception e) {
		e.printStackTrace();
	} finally {
		System.out.println("server closing");
	}
}

public static void propagateMessage(String msg) {
	//System.out.println("sendMessage(): " + msg.toString());
	
	//msg.incrementHops(nodeId);
	
		if (next != null){
			if(client==null){
			//call the client and forward the message next host and port
			client = new MessageClient(next, 4567);}
			System.out.println("I am here");
			client.ping();
			//next.message(msg);
			}
		else if (previous != null) {
			if(client==null){
				//call the client and forward the message next host and port
				client = new MessageClient(next, 4567);}

			client.ping();
		}
	
}
public static void propagateMessage() {
	//System.out.println("sendMessage(): " + msg.toString());
	
	//msg.incrementHops(nodeId);
	
		if (next != null){
			if(client==null){
			//call the client and forward the message next host and port
			client = new MessageClient(next, 4567);}
			System.out.println("I am here");
			client.ping();
			//next.message(msg);
			}
		else if (previous != null) {
			if(client==null){
				//call the client and forward the message next host and port
				client = new MessageClient(next, 4567);}

			client.ping();
		}
	
}
}



