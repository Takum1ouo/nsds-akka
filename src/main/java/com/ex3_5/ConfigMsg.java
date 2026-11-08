package com.ex3_5;

import akka.actor.ActorRef;

public class ConfigMsg {
	
	private final ActorRef server;

	public ConfigMsg(ActorRef server) {
		this.server = server;
	}

	public ActorRef getServer() {
		return server;
	}
}