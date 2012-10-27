package ar.edu.itba.pod.legajo51190.impl;

import java.util.Collection;
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

import com.google.common.collect.Lists;

public class NodeReceiver extends BaseJGroupNodeReceiver {

	private final NodeUpdateService updateService;
	private final Node node;
	private final BlockingQueue<Callable<Void>> connectionPendingTasks;
	private final ExecutorService connectionService;
	private final NodeLogger nodeLogger;

	public NodeReceiver(final Node node) {
		this.node = node;
		updateService = new NodeUpdateService(node);
		connectionService = Executors.newCachedThreadPool();
		connectionPendingTasks = new LinkedBlockingQueue<>();
		nodeLogger = new NodeLogger(node);
	}

	@Override
	public void viewAccepted(final View new_view) {
		updateService.updateFromView(new_view);
	}

	@Override
	public void suspect(final Address suspected_mbr) {
		nodeLogger.log("Member might have gone: " + suspected_mbr);
	}

	@Override
	public void receive(final Message msg) {

		if (msg.getObject() instanceof GlobalSyncNodeMessage) {
			synchronized (node) {
				onNewNodeSync(msg, (GlobalSyncNodeMessage) msg.getObject());
			}
		} else if (msg.getObject() instanceof GlobalSyncNodeMessageAnswer) {
			onNewNodeSyncAnswer((GlobalSyncNodeMessageAnswer) msg.getObject());
		}
	}

	private void onNewNodeSyncAnswer(final GlobalSyncNodeMessageAnswer message) {
		updateService.notifyNodeAnswer(message);
	}

	private void onNewNodeSync(final Message msg,
			final GlobalSyncNodeMessage message) {

		if (message.getDestinations().contains(node.getAddress())) {
			// We got all the new signals, so we save them
			synchronized (node.getLocalSignals()) {
				node.getLocalSignals().addAll(
						message.getSignalsMap().get(node.getAddress()));
			}
		}

		synchronized (node.getBackupSignals()) {
			// Copy mode is for when no backups are done
			// If backups are already distributed then we proceed from this
			// branch
			if (!message.isCopyMode()) {

				if (message.getDestinations().contains(node.getAddress())) {
					// If i'm one of the original receivers of this data,
					// Then I save my backup copies
					for (Address addr : message.getBackupSignals().keySet()) {
						if (!addr.equals(node.getAddress())) {
							node.getBackupSignals().putAll(msg.getSrc(),
									message.getBackupSignals().get(addr));
						}
					}
				} else {

					for (Address addr : node.getBackupSignals().keySet()) {
						nodeLogger.log("I HAD: "
								+ node.getBackupSignals().get(addr).size()
								+ " FOR: " + addr);
					}

					Collection<Signal> signals = Lists.newArrayList(node
							.getBackupSignals().get(msg.getSrc()));

					nodeLogger
							.log("IM GONNA REMOVE:"
									+ message.getSignalsMap().values().size()
									+ "nodes");

					node.getBackupSignals().removeAll(msg.getSrc());

					signals.removeAll(message.getSignalsMap().values());

					for (Address address : message.getSignalsMap().keySet()) {
						node.getBackupSignals().putAll(address,
								message.getSignalsMap().get(address));
					}

					node.getBackupSignals().putAll(msg.getSrc(), signals);

				}
			} else {
				// If this is a copy mode operation then each
				// Receiver stores it's data as backup
				for (Address addr : message.getBackupSignals().keySet()) {
					if (addr.equals(node.getAddress())) {
						node.getBackupSignals().putAll(msg.getSrc(),
								message.getBackupSignals().get(addr));
					}
				}
			}

		}
		final Message reply = msg.makeReply();
		reply.setObject(new GlobalSyncNodeMessageAnswer(node.getAddress()));
		sendSafeAnswer(reply);

		if (message.getDestinations().contains(node.getAddress())) {
			nodeLogger.log("I received " + node.getLocalSignals().size()
					+ " as a grand total");
		}
		if (message.isCopyMode()) {
			nodeLogger.log("I received " + node.getBackupSignals().size()
					+ " COPY signals");
		} else {
			for (Address addr : node.getBackupSignals().keySet()) {
				nodeLogger.log("I HAVE NOW: "
						+ node.getBackupSignals().get(addr).size() + " FOR: "
						+ addr);
			}
		}
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
