package ar.edu.itba.pod.legajo51190.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
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
import com.google.common.collect.Multimap;

public class NodeUpdateService {

	private final Multimap<Address, Signal> signalsKeptAsBackup = HashMultimap
			.create();
	private final BlockingQueue<Address> waitingAddresses = new LinkedBlockingQueue<>();
	private final ThreadPoolExecutor changeRequestService;
	private final Node node;
	private CountDownLatch latch;
	private final Timer dataUpdateTimer;
	private final AtomicBoolean timerEnabled = new AtomicBoolean(true);

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

		dataUpdateTimer = new Timer();
		dataUpdateTimer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {

				if (timerEnabled.get() && node.getChannel().isConnected()
						&& node.getAliveNodes().size() > 1) {
					final Set<Signal> signalsCopy = new HashSet<>();

					synchronized (node.getToDistributeSignals()) {
						signalsCopy.addAll(node.getToDistributeSignals());
						if (node.getToDistributeSignals().size() > 0) {
							synchronized (node) {

								Set<Address> allMembersButMyself = new HashSet<>(
										node.getAliveNodes());

								allMembersButMyself.remove(node.getAddress());

								syncNewMembers(
										allMembersButMyself,
										new ArrayList<Address>(node
												.getAliveNodes()), signalsCopy);
								node.getToDistributeSignals().clear();
							}
						}
					}
				}
			}
		}, 0, 1000);
	}

	public void updateFromView(final View new_view) {
		changeRequestService.submit(new Runnable() {
			@Override
			public void run() {
				Set<Address> newMembers = detectNewMembers(new_view);
				Set<Address> goneMembers = detectGoneMembers(new_view);

				synchronized (node) {
					if (goneMembers.size() > 0) {
						syncGoneMembers(goneMembers);
					}
					if (newMembers.size() > 0) {

						Set<Signal> signalsCopy = null;
						synchronized (node.getLocalSignals()) {
							signalsCopy = new HashSet<>(node.getLocalSignals());
						}

						syncNewMembers(newMembers, new_view.getMembers(),
								signalsCopy);
					}
					node.setNodeView(new_view);
				}
			}

		});
	}

	private void syncNewMembers(final Set<Address> newMembers,
			final List<Address> allMembers, final Set<Signal> signalsCopy) {

		Random r = new Random();

		Multimap<Address, Signal> signalsToSend = HashMultimap.create();
		Set<Signal> signalsToKeep = new HashSet<>();

		for (Signal signal : signalsCopy) {
			int index = r.nextInt(allMembers.size());
			Address addressToSendData = allMembers.get(index);
			if (newMembers.contains(addressToSendData)) {
				signalsToSend.put(addressToSendData, signal);
			} else {
				signalsToKeep.add(signal);
			}
		}

		sendSignalsToMembers(signalsToKeep, signalsToSend);
	}

	private void sendSignalsToMembers(final Set<Signal> signalsToKeep,
			final Multimap<Address, Signal> signalsToSend) {

		latch = new CountDownLatch(signalsToSend.keySet().size());

		for (Address addr : signalsToSend.keySet()) {
			try {
				Message msg = new Message(addr, new NodeMessage(
						new ArrayList<Signal>(signalsToSend.get(addr)),
						NodeMessage.MESSAGE_NEW_NODE_SYNC));
				signalsKeptAsBackup.putAll(addr, signalsToSend.get(addr));
				waitingAddresses.add(addr);
				node.getChannel().send(msg);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			latch.await(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// waitingAddresses will contain the hanged addresses.
			e.printStackTrace();
		}

		synchronized (node.getLocalSignals()) {
			for (Address addr : signalsToSend.keySet()) {
				node.getLocalSignals().removeAll(signalsToSend.get(addr));
			}
			node.getLocalSignals().addAll(signalsToKeep);
			// syncGoneMembers(new HashSet<Address>(waitingAddresses));
		}

		System.out.println("Now only " + node.getLocalSignals().size()
				+ " signals remain");
	}

	private void syncGoneMembers(final Set<Address> goneMembers) {
		if (goneMembers.size() > 1) {
			System.out
					.println("More than one member is gone, we might have lost messages :(");
		}
		synchronized (node.getLocalSignals()) {
			for (Address address : goneMembers) {
				System.out.println("Recovering backups from "
						+ address.toString() + " size: "
						+ signalsKeptAsBackup.get(address).size());
				node.getLocalSignals().addAll(signalsKeptAsBackup.get(address));
			}
		}
	}

	private Set<Address> detectGoneMembers(final View new_view) {
		Set<Address> goneMembers = new HashSet<Address>();
		for (Address address : node.getLastView().getMembers()) {
			if (!new_view.containsMember(address)) {
				// System.out.println("Hold member! bye! " + address);
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
				// System.out.println("New member! hey! " + address);
				node.setDegraded(true);
				newMembers.add(address);
			}
		}
		return newMembers;
	}

	public void notifyNodeAnswer(final NodeMessage message) {
		waitingAddresses.remove(message.getContent());
		latch.countDown();
	}
}
