package ar.edu.itba.pod.legajo51190.impl;

import org.jgroups.ChannelListener;
import org.jgroups.Receiver;

/**
 * Interface for all the methods a JGroupNode needs to have to work.
 * 
 * @author cris
 * 
 */
public interface JGroupNodeReceiver extends Receiver, ChannelListener {

}
