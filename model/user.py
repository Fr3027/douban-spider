from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class User(Base):
    __tablename__="user"
    id = Column(Integer, primary_key=True,autoincrement=True)
    username = Column(String)
    created = Column(DateTime)
    uid = Column(String)
    phone = Column(String)
    credibility = Column(String)
