package com.ex3_5;

import static akka.pattern.Patterns.ask;

import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class Client extends AbstractActor {
	
	private ActorRef server;
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ConfigMsg.class, this::onConfig)
				.match(PutMsg.class, this::onPut)
				.match(GetMsg.class, this::onGet)
				.build();
	}
	
	void onConfig(ConfigMsg msg) {
		System.out.println("CLIENT: got server address");
		server = msg.getServer();
	}
	
	void onPut(PutMsg msg) {
		System.out.println("CLIENT: sending " + msg.getName());
		server.tell(msg, getSelf());
	}
	
	void onGet(GetMsg msg) {
		System.out.println("CLIENT: asking for " + msg.getName());
		Future<Object> future = ask(server, msg, 5000);
		try {
			RepMsg reply = (RepMsg) Await.result(future, Duration.create(5, TimeUnit.SECONDS));
			if (reply.getEmail() != null) {
				System.out.println("CLIENT: " + reply.getName() + " -> " + reply.getEmail());
			} else {
				System.out.println("CLIENT: no email found for " + reply.getName());
			}
		} catch (Exception e) {
			System.out.println("CLIENT: no reply from server");
		}
	}
	static Props props() {
		return Props.create(Client.class);
	}
	
}
