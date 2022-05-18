from sqlalchemy import Column, Integer, String
from database import Base, SessionLocal

class Post(Base):
    __tablename__ = "post"
    #__table_args__ = {"schema": "public"}
    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)

if __name__ == "__main__":
    session = SessionLocal()
    result=[]
    for post in (session.query(Post).filter(Post.topic == "business").order_by(Post.id.desc()).limit(10).all()):
        result.append(post.id)
    print(result)



   