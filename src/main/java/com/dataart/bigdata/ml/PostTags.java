package com.dataart.bigdata.ml;

public class PostTags {
    private int postId;
    private String tagName;

    public PostTags(int postId, String tagName) {
        this.postId = postId;
        this.tagName = tagName;
    }

    public int getPostId() {
        return postId;
    }

    public void setPostId(int postId) {
        this.postId = postId;
    }

    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }
}
