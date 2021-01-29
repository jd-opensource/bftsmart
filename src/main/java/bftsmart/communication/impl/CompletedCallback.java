package bftsmart.communication.impl;

public interface CompletedCallback<S, R> {

	void onCompleted(S source, R result, Throwable error);

}