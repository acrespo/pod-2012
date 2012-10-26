package ar.edu.itba.pod.legajo51190.impl;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgroups.Address;
import org.jgroups.View;

import ar.edu.itba.pod.api.Signal;

public interface JGroupNode {

	public ConcurrentSkipListSet<String> getAliveNodeNames();

	public ConcurrentSkipListSet<Address> getAliveNodes();

	public AtomicBoolean getIsDegraded();

	public View getLastView();

	public Address getAddress();

	public Set<Signal> getLocalSignals();

	public Set<Signal> getToDistributeSignals();
}
