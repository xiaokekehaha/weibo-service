package zx.soft.weibo.sina.user.thread;

import java.io.IOException;

import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.weibo.sina.api.SinaWeiboAPI;

public class test {

	public static void main(String[] args) throws IOException {

		ThreadCore threadCore = new ThreadCore();
		SinaWeiboAPI api = new SinaWeiboAPI(new HttpClientDaoImpl());

		for (int i = 0; i < 16; i++) {
			threadCore.test(api);
			System.out.println("start thread " + i);
		}
	}
}