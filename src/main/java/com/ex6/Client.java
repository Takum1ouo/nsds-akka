package com.ex6;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Client extends AbstractActor {

	private ActorRef server;

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(TextMsg.class, this::onText)
				.match(ConfigMsg.class, this::onConfig)
				.build();
	}

	void onConfig(ConfigMsg msg) {
		System.out.println(getSelf().path().name() +": got server address");
		server = msg.getServer();
	}

	void onText(TextMsg msg) {
		if (getSender().equals(server)) {
			System.out.println(getSelf().path().name() + ": received -> " + msg.getText());
		} else {
			System.out.println(getSelf().path().name() +": sending " + msg.getText());
			server.tell(msg, getSelf());
		}
	}

	static Props props() {
		return Props.create(Client.class);
	}

}
