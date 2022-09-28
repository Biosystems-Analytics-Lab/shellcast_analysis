from sqlalchemy import (
        Column,
        String,
        Float
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Leases(Base):
    __tablename__ = 'Leases'

    lease_id = Column(String, primary_key=True)
    grow_area_name = Column(String, nullable=True)
    grow_area_desc = Column(String, nullable=True)
    cmu_name = Column(String, nullable=True)
    rainfall_thresh_in = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    def __init__(self, lease_id, grow_area_name, grow_area_desc, cmu_name, rainfall_thresh_in, latitude, longitude):
        self.lease_id = lease_id
        self.grow_area_name = grow_area_name
        self.grow_area_desc = grow_area_desc
        self.cmu_name = cmu_name
        self.rainfall_thresh_in = rainfall_thresh_in
        self.latitude = latitude
        self.longitude = longitude

    def ____repr__(self):
        return f'<Leases(\
        lease_id={self.lease_id},\
        grow_area_name={self.grow_area_name},\
        grow_area_desc={self.grow_area_desc},\
        cmu_name={self.cmu_name},\
        rainfall_thresh_in={self.rainfall_thresh_in},\
        latitude={self.latitude},\
        longitude={self.longitude})>'
