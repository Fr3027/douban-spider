from sqlalchemy import func
from sqlalchemy.sql.selectable import subquery

from model.topic import Topic
from model.user import User
from utils import Utils


class UserAnalysis(object):
    session = Utils.get_session()
    @classmethod
    def markS(cls):
        cls.session.query(User).update({'credibility':'N'})
        subquery = cls.session.query(Topic.uid).group_by(Topic.uid).having(func.count(Topic.id)==1).subquery()
        cls.session.query(User).filter(User.uid.in_(subquery)).update({'credibility':'S'},synchronize_session='fetch')
        cls.session.commit()
        # user_list  = cls.session.query(Topic.uid).group_by(Topic.uid).having(func.count(Topic.id)==1)
        # uids = [user.uid for user in user_list]
        # Utils.get_session().query(Topic.uid,func.count(Topic.id)).filter(Topic.uid.in_(uids)).group_by(Topic.uid).order_by(func.count(Topic.id).desc()).all()
        # cls.session.query(Topic.uid,func.count(Topic.id)).filter(Topic.uid.in_(uids)).group_by(Topic.uid).order_by(func.count(Topic.id).desc()).all()

UserAnalysis.markS()