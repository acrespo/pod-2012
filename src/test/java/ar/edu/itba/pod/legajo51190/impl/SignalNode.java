package ar.edu.itba.pod.legajo51190.impl;

import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.SignalProcessor;

public interface SignalNode extends SignalProcessor, SPNode {

	CountdownSyncListener getInjectedListener();
}
