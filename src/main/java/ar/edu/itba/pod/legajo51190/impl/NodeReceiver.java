package ar.edu.itba.pod.legajo51190.impl;

import java.io.Serializable;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.View;

import ar.edu.itba.pod.api.Signal;

public class NodeReceiver extends BaseJGroupNodeReceiver {

	private final NodeUpdateService updateService;
	private final Node node;

	public NodeReceiver(final Node node) {
		this.node = node;
		updateService = new NodeUpdateService(node);
	}

	@Override
	public void viewAccepted(final View new_view) {
		updateService.updateFromView(new_view);
	}

	@Override
	public void suspect(final Address suspected_mbr) {
		System.out.println("Member might have gone: " + suspected_mbr);
	}

	@Override
	public void receive(final Message msg) {
		NodeMessage message = (NodeMessage) msg.getObject();
		switch (message.getType()) {
		case NodeMessage.MESSAGE_NEW_NODE_SYNC:
			onNewNodeSync(msg, message);
			break;
		case NodeMessage.MESSAGE_NEW_NODE_SYNC_ANSWER:
			System.out.println("Got new node sync answer");
			onNewNodeSyncAnswer(message);
			break;
		default:
			System.out.println("Unknown message received!");
			break;
		}
	}

	private void onNewNodeSyncAnswer(final NodeMessage message) {
		updateService.notifyNodeAnswer(message);
	}

	@SuppressWarnings("unchecked")
	private void onNewNodeSync(final Message msg, final NodeMessage message) {
		List<Signal> signals = (List<Signal>) message.getContent();
		synchronized (node.getSignals()) {
			node.getSignals().addAll(signals);
		}

		System.out.println("I got " + signals.size() + " signals");
		Message reply = msg.makeReply();
		reply.setObject(new NodeMessage((Serializable) node.getAddress(),
				NodeMessage.MESSAGE_NEW_NODE_SYNC_ANSWER));
		try {
			node.getChannel().send(reply);
			System.out.println("Sending new node answer");
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Now I have " + node.getSignals().size());
	}

	@Override
	public void channelConnected(final Channel channel) {
		node.setNodeAddress(channel.getAddress());
		node.setNodeView(channel.getView());
	}

}
