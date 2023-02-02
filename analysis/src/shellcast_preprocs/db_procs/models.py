from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, DateTime, DECIMAL
from datetime import datetime
import json

Base = declarative_base()


class Cmu(Base):
    __tablename__ = 'cmus'

    id = Column(String(10), primary_key=True, nullable=False, unique=True)
    cmu_name = Column(String(10), nullable=False)
    created = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return 'CMU<id={0}, cmu_name={1}, created={2}>'.format(self.id, self.cmu_name, self.created)


class Lease(Base):
    __tablename__ = 'leases'

    lease_id = Column(String(20), primary_key=True, nullable=False, unique=True)
    grow_area_name = Column(String(3))
    grow_area_desc = Column(String(50))
    cmu_name = Column(String(10), nullable=False)
    rainfall_thresh_in = Column(DECIMAL(precision=2, scale=3))
    latitude = Column(DECIMAL(precision=8, scale=10))
    longitude = Column(DECIMAL(precision=8, scale=11))
    created = Column(DateTime, default=datetime.now)
    updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __str__(self):
        data = {
            'lease_id': self.lease_id,
            'grow_area_name': self.grow_area_name,
            'grow_area_desc': self.grow_area_desc,
            'cmu_name': self.cmu_name,
            'rainfall_thresh_in': str(self.rainfall_thresh_in),
            'latitude': str(self.latitude),
            'longitude': str(self.longitude),
            'created': self.created.strftime('%m-%d-%Y, %H:%M:%S'),
            'updated': self.updated.strftime('%m-%d-%Y, %H:%M:%S')
        }
        return json.dumps(data)

    def to_dict(self):
        return dict(
            lease_id=self.lease_id,
            grow_area_name=self.grow_area_name,
            grow_area_desc=self.grow_area_desc,
            cmu_name=self.cmu_name,
            rainfall_thresh_in=round(self.rainfall_thresh_in, 2),
            latitude=round(self.latitude, 8),
            longitude=round(self.longitude, 8)
        )
