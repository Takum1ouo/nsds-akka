package com.ex3_5;

import static akka.pattern.Patterns.ask;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class Main {

	public static void main(String[] args) {

		final ActorSystem sys = ActorSystem.create("System");
		final ActorRef supervisor = sys.actorOf(Supervisor.props(), "supervisor");
		final ActorRef client = sys.actorOf(Client.props(), "client");

		ActorRef server = null;
		try {
			Future<Object> future = ask(supervisor, new GetServerMsg(), 5000);
			server = (ActorRef) Await.result(future, Duration.create(5, TimeUnit.SECONDS));
		} catch (Exception e) {
			e.printStackTrace();
		}

		client.tell(new ConfigMsg(server), ActorRef.noSender());

		client.tell(new PutMsg("Luca", "luca.mottola@polimi.it"), ActorRef.noSender());
		client.tell(new PutMsg("Alessandro", "alessandro.margara@polimi.it"), ActorRef.noSender());

		client.tell(new PutMsg("Fail!", "x"), ActorRef.noSender());

		client.tell(new GetMsg("Luca"), ActorRef.noSender());
		client.tell(new GetMsg("Gianpaolo"), ActorRef.noSender());

		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		sys.terminate();
	}
}
