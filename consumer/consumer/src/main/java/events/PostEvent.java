package events;

/**
 * Created by Caim03 on 20/06/18.
 */
public class PostEvent extends Event {
    private long postId;
    private long userId;
    private String post;
    private String user;

    public PostEvent(String record) {
        super(record.split("\\|")[0]);

        String[] arrayList = record.split("\\|");
        this.postId = Long.valueOf(arrayList[1]);
        this.userId = Long.valueOf(arrayList[2]);
        this.post = arrayList[3];
        this.user = arrayList[4];
    }

    public long getPostId() {
        return postId;
    }

    public void setPostId(long postId) {
        this.postId = postId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getPost() {
        return post;
    }

    public void setPost(String post) {
        this.post = post;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
