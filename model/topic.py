from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Topic(Base):
    __tablename__="topic"
    id = Column(Integer, primary_key=True,autoincrement=True)
    like_count = Column(Integer)
    comments_count = Column(Integer)
    title = Column(String)
    alt = Column(String,unique=True)
    username = Column(String)
    content = Column(String)
    photos = Column(String)
    author = Column(String)
    groupname =Column(String)
    updated = Column(DateTime)
    created = Column(DateTime)
    is_agent = Column(String)
    uid = Column(String)

