package com.ex3_5;

import java.time.Duration;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;

public class Supervisor extends AbstractActor {

	final ActorRef server = getContext().actorOf(Server.props(), "server");

	private static SupervisorStrategy strategy =
			new OneForOneStrategy(
					1,
					Duration.ofMinutes(1),
					DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.resume())
							.build());

	@Override
	public SupervisorStrategy supervisorStrategy() {
		return strategy;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(GetServerMsg.class, this::onGetServer)
				.build();
	}

	void onGetServer(GetServerMsg msg) {
		getSender().tell(server, getSelf());
	}

	static Props props() {
		return Props.create(Supervisor.class);
	}

}
