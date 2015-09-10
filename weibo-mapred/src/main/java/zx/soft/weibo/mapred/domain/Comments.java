package zx.soft.weibo.mapred.domain;

import java.util.Date;

public class Comments {

	private long id;
	private Date created_at;
	private String text;
	private long uid;
	private String screen_name;
	private int source_type;
	private String original_id;
	private long original_uid;
	private String original_screen_name;

	public Comments() {

	}

	public Comments(Builder builder) {
		this.id = builder.id;
		this.created_at = builder.created_at;
		this.text = builder.text;
		this.uid = builder.uid;
		this.screen_name = builder.screen_name;
		this.source_type = builder.source_type;
		this.original_id = builder.original_id;
		this.original_uid = builder.original_uid;
		this.original_screen_name = builder.original_screen_name;

	}

	public static class Builder {

		private long id;
		private Date created_at;
		private String text;
		private long uid;
		private String screen_name;
		private int source_type;
		private String original_id;
		private long original_uid;
		private String original_screen_name;

		public Builder(long id, Date created_at, String text, long uid, String screen_name, int source_type,
				String original_id, long original_uid, String original_screen_name) {
			super();
			this.id = id;
			this.created_at = created_at;
			this.text = text;
			this.uid = uid;
			this.screen_name = screen_name;
			this.source_type = source_type;
			this.original_id = original_id;
			this.original_uid = original_uid;
			this.original_screen_name = original_screen_name;

		}

		public Comments build() {
			return new Comments(this);
		}
	}

	public long getId() {
		return id;
	}

	public Date getCreated_at() {
		return created_at;
	}

	public String getText() {
		return text;
	}

	public long getUid() {
		return uid;
	}

	public String getScreen_name() {
		return screen_name;
	}

	public int getSource_type() {
		return source_type;
	}

	public String getOriginal_id() {
		return original_id;
	}

	public long getOriginal_uid() {
		return original_uid;
	}

	public String getOriginal_screen_name() {
		return original_screen_name;
	}

	public void setId(long id) {
		this.id = id;
	}

	public void setCreated_at(Date created_at) {
		this.created_at = created_at;
	}

	public void setText(String text) {
		this.text = text;
	}

	public void setUid(long uid) {
		this.uid = uid;
	}

	public void setScreen_name(String screen_name) {
		this.screen_name = screen_name;
	}

	public void setSource_type(int source_type) {
		this.source_type = source_type;
	}

	public void setOriginal_id(String original_id) {
		this.original_id = original_id;
	}

	public void setOriginal_uid(long original_uid) {
		this.original_uid = original_uid;
	}

	public void setOriginal_screen_name(String original_screen_name) {
		this.original_screen_name = original_screen_name;
	}

	@Override
	public String toString() {
		return "Comments [id=" + id + ", created_at=" + created_at + ", text=" + text + ", uid=" + uid
				+ ", screen_name=" + screen_name + ", source_type=" + source_type + ", original_id=" + original_id
				+ ", original_uid=" + original_uid + ", original_screen_name=" + original_screen_name + "]";
	}

}
