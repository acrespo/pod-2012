package ar.edu.itba.pod.legajo51190.impl;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgroups.Address;
import org.jgroups.View;

public interface JGroupNode {

	public ConcurrentSkipListSet<String> getAliveNodeNames();

	public ConcurrentSkipListSet<Address> getAliveNodes();

	public AtomicBoolean getIsDegraded();

	public View getLastView();

	public Address getAddress();
}
