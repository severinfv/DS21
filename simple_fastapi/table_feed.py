from sqlalchemy import Column, ForeignKey, Integer, String, TIMESTAMP
from sqlalchemy.orm import relationship
from database import Base, SessionLocal
from table_user import User
from table_post import Post

class Feed(Base):
    __tablename__ = "feed_action"
    
    action = Column(String)
    post_id = Column(Integer, ForeignKey(Post.id), primary_key=True, name = "post_id")
    post = relationship("Post")
    time = Column(TIMESTAMP)
    user_id = Column(Integer, ForeignKey(User.id), primary_key=True, name = "user_id")
    user = relationship("User")

if __name__ == "__main__":
    session = SessionLocal()