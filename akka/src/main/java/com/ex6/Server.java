package com.ex6;

import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;

public class Server extends AbstractActorWithStash {
	
	private String current;
	
	@Override
	public Receive createReceive() {
		return ready();
	}
	
	public Receive ready() {
		return receiveBuilder()
				.match(TextMsg.class, this::startProcessing)
				.build();
	}
	
	public Receive busy() {
		return receiveBuilder()
				.match(TextMsg.class, this::bufferText)
				.match(DoneMsg.class, this::finishProcessing)
				.build();
	}
	
	void startProcessing(TextMsg msg) {
		current = msg.getText().toUpperCase();
		System.out.println(current);
		getContext().system().scheduler().scheduleOnce(
				Duration.create(2, TimeUnit.SECONDS),
				getSelf(),
				new DoneMsg(),
				getContext().dispatcher(),
				getSender());

		getContext().become(busy());
	}

	void bufferText(TextMsg msg) {
		System.out.println("SERVER: busy, stashed " + msg.getText());
		stash();
	}
	
	void finishProcessing(DoneMsg msg) {		
		getSender().tell(new TextMsg(current), getSelf());
		getContext().become(ready());
		unstashAll();
	}
	static Props props() {
		return Props.create(Server.class);
	}
		
}