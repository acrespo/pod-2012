package ar.edu.itba.pod.legajo51190.impl;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.View;

import ar.edu.itba.pod.api.Signal;

public class NodeReceiver extends BaseJGroupNodeReceiver {

	private final NodeUpdateService updateService;
	private final Node node;
	private final BlockingQueue<Callable<Void>> connectionPendingTasks;
	private final ExecutorService connectionService;

	public NodeReceiver(final Node node) {
		this.node = node;
		updateService = new NodeUpdateService(node);
		connectionService = Executors.newCachedThreadPool();
		connectionPendingTasks = new LinkedBlockingQueue<>();
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
			System.out.println("Received new node sync message");
			onNewNodeSync(msg, message);
			break;
		case NodeMessage.MESSAGE_NEW_NODE_SYNC_ANSWER:
			System.out.println("Received new node sync message answer");
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
		synchronized (node.getLocalSignals()) {
			node.getLocalSignals().addAll(signals);
		}

		System.out.println("I got " + signals.size() + " signals");
		final Message reply = msg.makeReply();
		reply.setObject(new NodeMessage((Serializable) node.getAddress(),
				NodeMessage.MESSAGE_NEW_NODE_SYNC_ANSWER));
		sendSafeAnswer(reply);

		System.out.println("Now I have " + node.getLocalSignals().size());
	}

	private void sendSafeAnswer(final Message reply) {
		try {
			synchronized (connectionPendingTasks) {
				if (node.getChannel().isConnected()) {
					node.getChannel().send(reply);
				} else {
					connectionPendingTasks.add(new Callable<Void>() {
						@Override
						public Void call() {
							try {
								node.getChannel().send(reply);
							} catch (Exception e) {
								e.printStackTrace();
							}
							return null;
						}
					});
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void channelConnected(final Channel channel) {
		synchronized (connectionPendingTasks) {
			node.setNodeAddress(channel.getAddress());
			node.setNodeView(channel.getView());
			for (Callable<Void> task : connectionPendingTasks) {
				connectionService.submit(task);
			}
			connectionPendingTasks.clear();
		}

		System.out.println("Connected!");
	}

}
