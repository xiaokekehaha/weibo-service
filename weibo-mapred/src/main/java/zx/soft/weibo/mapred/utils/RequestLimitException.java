package zx.soft.weibo.mapred.utils;

public class RequestLimitException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = -1916088416950121954L;

	public RequestLimitException() {
		super();
	}

	public RequestLimitException(String msg) {
		super(msg);
	}
}
