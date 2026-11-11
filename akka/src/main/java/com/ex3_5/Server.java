package com.ex3_5;

import java.util.HashMap;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class Server extends AbstractActor {
	
	private final HashMap<String, String> contacts = new HashMap<>();
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(PutMsg.class, this::onPut)
				.match(GetMsg.class, this::onGet)
				.build();
	}
	
	void onPut(PutMsg msg) throws Exception {
		if (msg.getName().equals("Fail!")) {
			throw new Exception("Server failing!");
		}
		System.out.println("SERVER: stored " + msg.getName() + " " + msg.getEmail());
		contacts.put(msg.getName(), msg.getEmail());
	}
	
	void onGet(GetMsg msg) {
		System.out.println("SERVER: query for " + msg.getName());
		String email = contacts.get(msg.getName());
		getSender().tell(new RepMsg(msg.getName(),email), getSelf());
	}
	
	static Props props() {
		return Props.create(Server.class);
	}
	
	
}