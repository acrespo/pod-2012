package ar.edu.itba.pod.legajo51190.api;

import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.SignalProcessor;
import ar.edu.itba.pod.legajo51190.impl.CountdownSyncListener;

public interface SignalNode extends SignalProcessor, SPNode {

	CountdownSyncListener getInjectedListener();
}
