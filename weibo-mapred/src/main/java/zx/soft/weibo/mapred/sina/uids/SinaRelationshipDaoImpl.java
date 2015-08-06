package zx.soft.weibo.mapred.sina.uids;

import zx.soft.utils.http.ClientDao;
import zx.soft.weibo.mapred.domain.UsersAndIds;
import zx.soft.weibo.mapred.sina.FriendshipsDetail;
import zx.soft.weibo.sina.api.SinaWeiboAPI;

public class SinaRelationshipDaoImpl implements SinaRelationshipDao {

	SinaWeiboAPI api;

	public SinaRelationshipDaoImpl(ClientDao clientDao) {
		this.api = new SinaWeiboAPI(clientDao);
	}

	@Override
	public UsersAndIds getFollowers(String uid, String source) {
		return FriendshipsDetail.getFriendships(api, uid, Boolean.TRUE, source);
	}

	@Override
	public UsersAndIds getFriends(String uid, String source) {
		return FriendshipsDetail.getFriendships(api, uid, Boolean.FALSE, source);
	}

}
