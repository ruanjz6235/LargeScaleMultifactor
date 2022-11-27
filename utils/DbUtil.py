#! /usr/bin/env python
# -*- coding: utf-8 -*-
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import sessionmaker, scoped_session
from extensions import db
import extensions
from test.local import LocalDb
from flask import current_app


class DbUtil:

    @classmethod
    def get_app(cls):
        return db.get_app()

    @classmethod
    def get_engine(cls, bind=None):
        if extensions.start_app:
           # db = SQLAlchemy(current_app)
           engine = db.get_engine(app=db.get_app(), bind=bind)
        else:
           engine = LocalDb.get_engine(bind)
        return engine

    @classmethod
    def get_conn(cls, bind=None):
        engine = cls.get_engine(bind=bind)
        conn = engine.raw_connection()
        # session_factory = sessionmaker(bind=engine)
        # Session = scoped_session(session_factory)
        # conn = Session.bind
        return conn

    @classmethod
    def get_scoped_session(cls, bind=None):
        engine = cls.get_engine(bind=bind)
        return scoped_session(sessionmaker(autocommit=False,
                                           autoflush=True,
                                           bind=engine))

    @classmethod
    def get_session(cls, bind=None):
        engine = cls.get_engine(bind=bind)
        Session = sessionmaker(bind=engine)
        return Session()


