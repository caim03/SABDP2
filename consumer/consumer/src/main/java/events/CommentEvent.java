package events;

/**
 * Created by Caim03 on 20/06/18.
 */
public class CommentEvent extends Event {
    private long commentId;
    private long userId;
    private String comment;
    private String user;
    private long commentReplied;
    private long postCommented;

    public CommentEvent(String record) {
        super(record.split("\\|")[0]);

        String[] arrayList = record.split("\\|");
        this.commentId = Long.valueOf(arrayList[1]);
        this.userId = Long.valueOf(arrayList[2]);
        this.comment = arrayList[3];
        this.user = arrayList[4];
        this.commentReplied = Long.valueOf(arrayList[5]);
        this.postCommented = Long.valueOf(arrayList[6]);
    }

    public long getCommentId() {
        return commentId;
    }

    public void setCommentId(long commentId) {
        this.commentId = commentId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public long getCommentReplied() {
        return commentReplied;
    }

    public void setCommentReplied(long commentReplied) {
        this.commentReplied = commentReplied;
    }

    public long getPostCommented() {
        return postCommented;
    }

    public void setPostCommented(long postCommented) {
        this.postCommented = postCommented;
    }
}
