package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.View;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Signal;

import com.google.common.collect.Multimap;

/**
 * Represents the internal structure of a JGroup for my implementation. It
 * contains all the relevant data for the management of data inside a processor.
 * 
 * @author cris
 */
public interface JGroupNode {

	public Set<String> getAliveNodeNames();

	public Set<Address> getAliveNodes();

	public View getLastView();

	public Address getAddress();

	public Set<Signal> getLocalSignals();

	public Set<Signal> getRedistributionSignals();

	public BlockingQueue<Signal> getToDistributeSignals();

	public Multimap<Address, Signal> getBackupSignals();

	public NodeStats getStats();

	public NodeListener getListener();

	public boolean isOnline();

	public void joinChannel(String name) throws RemoteException;

	public Channel getChannel();

	JGroupSignalProcessor getSignalProcessor();

	Set<Signal> getTemporalSignals();

	void reset() throws Exception;
}
