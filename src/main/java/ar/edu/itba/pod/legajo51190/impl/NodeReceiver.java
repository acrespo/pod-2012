package ar.edu.itba.pod.legajo51190.impl;

import java.util.List;
import java.util.Set;
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
import com.google.common.collect.Sets;

public class NodeReceiver extends BaseJGroupNodeReceiver {

	private final NodeUpdateService updateService;
	private final Node node;
	private final BlockingQueue<Callable<Void>> connectionPendingTasks;
	private final ExecutorService connectionService;
	private final ExecutorService workerService;
	private final NodeLogger nodeLogger;

	public NodeReceiver(final Node node) {
		this.node = node;
		updateService = new NodeUpdateService(node);
		connectionService = Executors.newCachedThreadPool();
		connectionPendingTasks = new LinkedBlockingQueue<>();
		nodeLogger = new NodeLogger(node);
		workerService = Executors.newSingleThreadExecutor();
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
			if (node.getChannel().isConnected()) {
				workerService.submit(new Runnable() {
					@Override
					public void run() {
						onNewNodeSync(msg,
								(GlobalSyncNodeMessage) msg.getObject());
					}
				});
			} else {
				connectionPendingTasks.add(new Callable<Void>() {
					@Override
					public Void call() {
						try {
							workerService.submit(new Runnable() {

								@Override
								public void run() {
									onNewNodeSync(msg,
											(GlobalSyncNodeMessage) msg
													.getObject());
								}
							});

						} catch (Exception e) {
							e.printStackTrace();
						}
						return null;
					}
				});
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

		synchronized (node.getBackupSignals()) {
			synchronized (node.getLocalSignals()) {
				if (message.getDestinations().contains(node.getAddress())) {
					// We got all the new signals, so we save them
					node.getLocalSignals().addAll(
							message.getSignalsMap().get(node.getAddress()));
				}
			}

			// Copy mode is for when no backups are done
			// If backups are already distributed then we proceed from this
			// branch
			if (!message.isCopyMode()) {

				if (message.getDestinations().contains(node.getAddress())) {
					// If i'm one of the original receivers of this data,
					// Then I save my backup copies
					for (Address addr : message.getBackupSignals().keySet()) {
						node.getBackupSignals().putAll(addr,
								message.getBackupSignals().get(addr));
					}
				} else {

					Set<Signal> signals = Sets.newHashSet(node
							.getBackupSignals().get(msg.getSrc()));

					node.getBackupSignals().removeAll(msg.getSrc());

					Set<Signal> signalsIntersection = Sets.intersection(
							signals,
							Sets.newHashSet(message.getSignalsMap().values()));
					signals.removeAll(message.getSignalsMap().values());

					List<Address> allThirdKindMembers = Lists.newArrayList(node
							.getAliveNodes());
					List<Address> allMembers = Lists.newArrayList(node
							.getAliveNodes());

					allThirdKindMembers.removeAll(message.getDestinations());
					allThirdKindMembers.remove(msg.getSrc());

					for (Address address : message.getSignalsMap().keySet()) {

						for (Signal signal : message.getSignalsMap().get(
								address)) {
							Address owner = updateService.getAddressForSignal(
									signal, allThirdKindMembers, allMembers);

							if (owner.equals(node.getAddress())) {
								node.getBackupSignals().put(address, signal);
							} else {
								nodeLogger.logAcum(owner.toString());
							}

						}
						// node.getBackupSignals().putAll(address,
						// message.getSignalsMap().get(address));

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

		// if (message.getDestinations().contains(node.getAddress())) {
		// nodeLogger.log("I got message from " + msg.getSrc());
		// }
		// if (message.isCopyMode()) {
		// nodeLogger.log("I received " + node.getBackupSignals().size()
		// + " COPY signals");
		// } else {
		// nodeLogger.logAcum("I have " + node.getLocalSignals().size()
		// + " signals");
		// if (!message.isCopyMode()) {
		// nodeLogger.logAcum("I got total " + node.getBackupSignals().size()
		// + " from " + msg.getSrc());
		// }
		nodeLogger.flush();
		// }
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
