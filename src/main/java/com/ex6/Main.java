package com.ex6;

import java.io.IOException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class Main {

	public static void main(String[] args) {

		final ActorSystem sys = ActorSystem.create("System");
		final ActorRef server = sys.actorOf(Server.props(), "server");
		final ActorRef client1 = sys.actorOf(Client.props(), "client1");
		final ActorRef client2 = sys.actorOf(Client.props(), "client2");
		
		client1.tell(new ConfigMsg(server), ActorRef.noSender());
		client2.tell(new ConfigMsg(server), ActorRef.noSender());

		client1.tell(new TextMsg("a"), ActorRef.noSender());
		client2.tell(new TextMsg("b"), ActorRef.noSender());

		client1.tell(new TextMsg("c"), ActorRef.noSender());
		client2.tell(new TextMsg("d"), ActorRef.noSender());

		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		sys.terminate();
	}
}