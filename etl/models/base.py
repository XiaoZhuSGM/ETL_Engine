from sqlalchemy.ext.declarative import declared_attr
from datetime import datetime
from etl.etl import db

class CRUDMixin(object):
    """Mixin that adds convenience methods for CRUD (create, read, update, delete)
    operations.
    """

    @declared_attr
    def __tablename__(cls):

        class_name_str = cls.__name__
        table_name = class_name_str[0].lower()

        for character in class_name_str[1:]:
            table_name += character if character.islower() else '_' + character.lower()

        return table_name

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)


    @classmethod
    def create(cls, **kwargs):
        """Create a new record and save it the database."""
        instance = cls(**kwargs)
        instance.save()
        return instance

    @classmethod
    def create_batch(cls, arrays):
        """
        batch create record and save it the database.
        :return:
        """
        instances = []
        for item in arrays:
            instance = cls(**item)
            instances.append(instance)
        db.session.add_all(instances)
        return instances

    def update(self, **kwargs):
        """Update specific fields of a record."""
        for attr, value in kwargs.items():
            setattr(self, attr, value)
        self.save()
        return self

    def save(self):
        """Save the record."""
        db.session.add(self)
        db.session.flush()

    def delete(self):
        """Remove the record from the database."""
        db.session.delete(self)

    @staticmethod
    def flush():
        """ flush to DB not commit"""
        db.session.flush()

    def to_dict(self):
        return self.__dict__
