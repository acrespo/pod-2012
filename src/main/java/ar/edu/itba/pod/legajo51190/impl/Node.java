package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.View;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Signal;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class Node implements JGroupNode {
	private final Multimap<Address, Signal> backupSignals;
	private Set<Address> aliveNodes;
	private Set<String> aliveNodeNames;
	private View lastView;
	private Address nodeAddress;
	private final AtomicBoolean isDegraded = new AtomicBoolean(false);
	private final Set<Signal> signals = Collections
			.newSetFromMap(new ConcurrentHashMap<Signal, Boolean>());
	private final Set<Signal> toDistributeSignals = Collections
			.newSetFromMap(new ConcurrentHashMap<Signal, Boolean>());
	private final Set<Signal> redistributionSignals = Collections
			.newSetFromMap(new ConcurrentHashMap<Signal, Boolean>());
	private final Channel channel;
	private final NodeListener listener;
	private final AtomicBoolean online = new AtomicBoolean(false);

	public Node(final NodeListener listener) throws Exception {
		super();
		channel = new JChannel("udp-largecluster.xml");
		this.listener = listener;
		Multimap<Address, Signal> sig = HashMultimap.create();
		backupSignals = Multimaps.synchronizedMultimap(sig);
	}

	@Override
	public Address getAddress() {
		return nodeAddress;
	}

	@Override
	public Set<String> getAliveNodeNames() {
		return aliveNodeNames;
	}

	@Override
	public Set<Address> getAliveNodes() {
		return aliveNodes;
	}

	public Channel getChannel() {
		return channel;
	}

	@Override
	public AtomicBoolean getIsDegraded() {
		return isDegraded;
	}

	@Override
	public View getLastView() {
		return lastView;
	}

	@Override
	public Set<Signal> getLocalSignals() {
		return signals;
	}

	@Override
	public Set<Signal> getToDistributeSignals() {
		return toDistributeSignals;
	}

	public void setDegraded(final boolean b) {
		isDegraded.set(b);
	}

	public void setNodeAddress(final Address address) {
		nodeAddress = address;
	}

	public void setNodeView(final View view) {
		aliveNodes = new ConcurrentSkipListSet<>(view.getMembers());
		aliveNodeNames = new ConcurrentSkipListSet<String>(
				Collections2.transform(aliveNodes,
						new Function<Address, String>() {
							@Override
							public String apply(@Nullable final Address input) {
								return input.toString();
							}
						}));
		lastView = view;
	}

	@Override
	public Multimap<Address, Signal> getBackupSignals() {
		return backupSignals;
	}

	@Override
	public NodeStats getStats() {
		// TODO: Improve nodestats implementation
		return new NodeStats(getAddress().toString(), 0, signals.size()
				+ toDistributeSignals.size() + redistributionSignals.size(),
				backupSignals.size(), false);
	}

	@Override
	public NodeListener getListener() {
		return listener;
	}

	@Override
	public Set<Signal> getRedistribuitionSignals() {
		return redistributionSignals;
	}

	@Override
	public boolean isOnline() {
		return online.get();
	}

	@Override
	public void joinChannel(final String name) throws RemoteException {
		try {
			getChannel().connect(name);
			online.set(true);
		} catch (Exception e) {
			throw new RemoteException(e.getMessage());
		}

	}

	public void exit() {
		online.set(false);
		synchronized (getToDistributeSignals()) {
			synchronized (getLocalSignals()) {
				synchronized (getRedistribuitionSignals()) {
					getLocalSignals().clear();
					getToDistributeSignals().clear();
					getRedistribuitionSignals().clear();
				}
			}
		}
		getChannel().close();
	}
}
