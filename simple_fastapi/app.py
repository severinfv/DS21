from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from typing import List

from database import SessionLocal
from schema import  UserGet, PostGet, FeedGet
from table_user import User
from table_post import Post
from table_feed import Feed

app = FastAPI()

def get_db():
    with SessionLocal() as db:
        return db

@app.get("/post/recommendations/", response_model=List[PostGet])
def popular_posts(id: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    topposts =  db.query(Feed.post_id).filter(Feed.action == "like").group_by(Feed.post_id).order_by(func.count(Feed.post_id).desc()).limit(limit).subquery()
    return db.query(Post).filter(Post.id.in_(topposts)).all()

    

@app.get("/user/{id}", response_model=List[UserGet])
def get_user(id: int, db: Session = Depends(get_db)):
    result = db.query(User).filter(User.id == id).all()
    if not result:
        raise HTTPException(status_code=404, detail="User not found!")

    return result

@app.get("/post/{id}", response_model=List[PostGet])
def get_post(id: int, db: Session = Depends(get_db)):
    result = db.query(Post).filter(Post.id == id).all()
    if not result:
        raise HTTPException(status_code=404, detail="Post entry not found!")

    return result


@app.get("/user/{id}/feed", response_model=List[FeedGet])
def get_user_feed(id: int, limit: int = 10, db: Session = Depends(get_db)):
    result = db.query(Feed).filter(Feed.user_id == id).order_by(Feed.time.desc()).limit(limit).all()

    if not result:
        return []  # not raising exeption, as it returns status 200

    return result

@app.get("/post/{id}/feed", response_model=List[FeedGet])
def get_post_feed(id: int, limit: int = 10, db: Session = Depends(get_db)):
    result = db.query(Feed).filter(Feed.post_id == id).order_by(Feed.time.desc()).limit(limit).all()

    if not result:
        return []  # not raising exeption, as it returns status 200
        
    return result


                
