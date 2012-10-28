package ar.edu.itba.pod.legajo51190.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;

import ar.edu.itba.pod.api.Signal;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class NodeUpdateService {

	private final BlockingQueue<Address> waitingAddresses = new LinkedBlockingQueue<>();
	private final ThreadPoolExecutor changeRequestService;
	private final Node node;
	private CountDownLatch latch;
	private final Timer dataUpdateTimer;
	private final AtomicBoolean timerEnabled = new AtomicBoolean(true);
	private final NodeLogger nodeLogger;

	public NodeUpdateService(final Node node) {
		this.node = node;
		changeRequestService = new ThreadPoolExecutor(1, 1, 1,
				TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>()) {
			@Override
			protected void afterExecute(final Runnable r, final Throwable t) {
				node.setDegraded(getQueue().isEmpty());
				timerEnabled.set(getQueue().isEmpty());
			}
		};

		nodeLogger = new NodeLogger(node);

		dataUpdateTimer = new Timer();
		dataUpdateTimer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {

				try {

					if (timerEnabled.get() && node.getChannel().isConnected()
							&& node.getAliveNodes().size() > 1) {
						final Set<Signal> signalsCopy = new HashSet<>();

						synchronized (node.getToDistributeSignals()) {
							signalsCopy.addAll(node.getToDistributeSignals());
							if (node.getToDistributeSignals().size() > 0) {

								Set<Address> allMembersButMyself = new HashSet<>(
										node.getAliveNodes());

								allMembersButMyself.remove(node.getAddress());

								nodeLogger.log("Updating my new nodes...");
								syncNewMembers(
										Lists.newArrayList(allMembersButMyself),
										Lists.newArrayList(node.getAliveNodes()),
										signalsCopy, HashMultimap.create(node
												.getBackupSignals()));
								node.getToDistributeSignals().clear();

								nodeLogger.log("Updated!");
								if (node.getListener() != null) {
									node.getListener().onNodeSyncDone();
								}
							}

						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, 0, 1000);

	}

	public void updateFromView(final View new_view) {
		changeRequestService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					if (node.getLastView() != null) {

						Set<Address> newMembers = detectNewMembers(new_view);
						Set<Address> goneMembers = detectGoneMembers(new_view);

						if (goneMembers.size() > 0) {
							syncGoneMembers(goneMembers);
						}
						if (newMembers.size() > 0
								&& node.getLocalSignals().size() > 0) {

							Set<Signal> signalsCopy = null;
							synchronized (node.getLocalSignals()) {
								signalsCopy = new HashSet<>(node
										.getLocalSignals());
							}

							// nodeLogger.log("New node! Sending data...");
							syncNewMembers(
									Lists.newArrayList(newMembers),
									new_view.getMembers(),
									signalsCopy,
									HashMultimap.create(node.getBackupSignals()));

							nodeLogger.log("Updated!");
							if (node.getListener() != null) {
								node.getListener().onNodeSyncDone();
							}
						}

					}
					node.setNodeView(new_view);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}

		});
	}

	private void syncNewMembers(final List<Address> newMembers,
			final List<Address> allMembers, final Set<Signal> signalsCopy,
			final Multimap<Address, Signal> backupMapCopy) {

		Multimap<Address, Signal> signalsToSend = HashMultimap.create();

		Set<Signal> signalsToKeep = new HashSet<>();

		for (Signal signal : signalsCopy) {
			Address addressToSendData = getAddressForSignal(signal, allMembers,
					allMembers);
			if (newMembers.contains(addressToSendData)) {
				signalsToSend.put(addressToSendData, signal);
			} else {
				signalsToKeep.add(signal);
			}
		}

		List<Address> allMembersButMe = new ArrayList<>(allMembers);
		allMembersButMe.remove(node.getAddress());

		Multimap<Address, Signal> backupSignalsToSend = HashMultimap.create();

		boolean copyMode = allMembers.size() - newMembers.size() == 1;

		if (copyMode) {
			for (Signal signal : signalsToKeep) {
				Address addressToSendData = getAddressForSignal(signal,
						allMembersButMe, allMembers);
				backupSignalsToSend.put(addressToSendData, signal);
			}
		} else {
			List<Address> newMembersAndMe = new ArrayList<>(newMembers);
			newMembersAndMe.add(node.getAddress());
			for (Address backupAddr : backupMapCopy.keySet()) {
				for (Signal sign : backupMapCopy.get(backupAddr)) {
					Address addressToSendData = getAddressForSignal(sign,
							allMembersButMe, allMembers);
					if (!addressToSendData.equals(node.getAddress())
							&& newMembersAndMe.contains(addressToSendData)) {
						backupSignalsToSend.put(backupAddr, sign);
					}
				}
				nodeLogger.logAcum("Sending: "
						+ backupSignalsToSend.get(backupAddr).size()
						+ " nodes from " + backupAddr);
			}

		}

		nodeLogger.logAcum("Sending my " + signalsToSend.size());
		nodeLogger.logAcum("Sending backups total "
				+ backupSignalsToSend.size());
		nodeLogger.logAcum("Keeping " + signalsToKeep.size() + " for myself");
		nodeLogger.logAcum("I considered I had " + allMembers.size()
				+ " members in cluster and " + newMembers.size());

		nodeLogger.flush();

		sendSignalsToMembers(signalsToKeep, signalsToSend, backupSignalsToSend,
				copyMode, allMembersButMe, allMembers);
	}

	private void sendSignalsToMembers(final Set<Signal> signalsToKeep,
			final Multimap<Address, Signal> signalsToSend,
			final Multimap<Address, Signal> backupSignalsToSend,
			final boolean copyMode, final List<Address> receptors,
			final List<Address> allMembers) {

		latch = new CountDownLatch(receptors.size());

		try {

			for (Address receptor : receptors) {
				Message msg = new Message(receptor, new GlobalSyncNodeMessage(
						signalsToSend, backupSignalsToSend, copyMode,
						allMembers));
				node.getChannel().send(msg);
			}

			for (Address addr : signalsToSend.keySet()) {
				if (!addr.equals(node.getAddress())) {
					if (copyMode) {
						node.getBackupSignals().putAll(addr,
								signalsToSend.get(addr));
					}
				}
			}
			waitingAddresses.addAll(receptors);

		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			if (!latch.await(5, TimeUnit.SECONDS)) {
				throw new Exception("JA!!!");
			}
		} catch (Exception e) {
			for (Address address : waitingAddresses) {
				System.out.println("Sending to " + address);
				new Message(address, new GlobalSyncNodeMessage(signalsToSend,
						backupSignalsToSend, copyMode, allMembers));
			}

			latch = new CountDownLatch(waitingAddresses.size());

			try {
				if (!latch.await(5, TimeUnit.SECONDS)) {
					throw new Exception("JA!!!");
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}

		}

		synchronized (node.getLocalSignals()) {
			synchronized (node.getBackupSignals()) {
				for (Address addr : signalsToSend.keySet()) {
					node.getLocalSignals().removeAll(signalsToSend.get(addr));
				}

				for (Address addr : node.getBackupSignals().keySet()) {
					for (Signal signal : backupSignalsToSend.get(addr)) {
						node.getBackupSignals().remove(addr, signal);
					}
				}

				node.getLocalSignals().addAll(signalsToKeep);
				// syncGoneMembers(new HashSet<Address>(waitingAddresses));
			}
		}

	}

	private void syncGoneMembers(final Set<Address> goneMembers) {
		if (goneMembers.size() > 1) {
			nodeLogger
					.log("More than one member is gone, we might have lost messages :(");
		}
		synchronized (node.getBackupSignals()) {
			synchronized (node.getLocalSignals()) {
				for (Address address : goneMembers) {
					nodeLogger.log("Recovering backups from "
							+ address.toString() + " size: "
							+ node.getBackupSignals().get(address).size());
					node.getLocalSignals().addAll(
							node.getBackupSignals().get(address));
					node.getBackupSignals().removeAll(address);
				}
			}
		}
	}

	private Set<Address> detectGoneMembers(final View new_view) {
		Set<Address> goneMembers = new HashSet<Address>();
		for (Address address : node.getLastView().getMembers()) {
			if (!new_view.containsMember(address)) {
				nodeLogger.log("Gone member! bye! " + address);
				node.setDegraded(true);
				goneMembers.add(address);
			}
		}
		return goneMembers;
	}

	private Set<Address> detectNewMembers(final View new_view) {
		Set<Address> newMembers = new HashSet<Address>();
		for (Address address : new_view.getMembers()) {
			if (!node.getLastView().containsMember(address)) {
				// nodeLogger.log("New member! hey! " + address);
				node.setDegraded(true);
				newMembers.add(address);
			}
		}
		return newMembers;
	}

	// private static final Random r = new Random();

	Address getAddressForSignal(final Signal sig,
			final List<Address> membersToPutIn, final List<Address> allMembers) {
		int size = membersToPutIn.size();
		int allMembersSize = allMembers.size();
		int index = Math.abs(sig.hashCode()) % allMembersSize % size;
		return membersToPutIn.get(index);
	}

	public void notifyNodeAnswer(final GlobalSyncNodeMessageAnswer message) {
		waitingAddresses.remove(message.getOwner());
		latch.countDown();
	}
}
