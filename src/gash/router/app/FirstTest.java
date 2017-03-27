package gash.router.app;

import gash.router.client.MessageClient;

public class FirstTest {

public static void main(String[] args) {
	MessageClient client=new MessageClient("169.254.67.23",4567);
    client.ping();
}
}
