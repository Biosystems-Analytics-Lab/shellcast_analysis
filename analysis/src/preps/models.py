from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, DECIMAL
from datetime import datetime
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
    rainfall_thresh_in = Column(DECIMAL(3, 2))
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    created = Column(DateTime, default=datetime.now)
    updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f'Lease<lease_id={0}, \
        grow_area_name={1}, \
        grow_area_desc={2}, \
        cmu_name={3},\
        rainfall_thresh_in={4}, \
        latitude={5}, \
        longitude={6}, \
        created={7} \
        updated={8}>'.format(self.lease_id, self.grow_area_name, self.grow_area_desc,
                             self.cmu_name, self.rainfall_thresh_in, self.latitude,
                             self.longitude, self.created, self.updated)


