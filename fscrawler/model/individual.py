import sqlite3 as sl
from enum import Enum
from typing import Optional

from .name import Name


class Gender(Enum):
    Male = -1
    Unknown = 0
    Female = 1


class Individual:

    counter = 0

    def __init__(self, data, iteration):
        self.fid: str = data["id"]
        self.name: Optional[Name]
        self.gender: Gender = Gender.Unknown
        self.living: bool = False
        self.iteration: int = iteration
        self.lifespan: str
        if isinstance(data, sl.Row):
            self._add_data_from_db(data)
        else:
            self._add_data(data)

    def _add_data_from_db(self, data):
        """ add individual data from DB"""
        if "color" in data.keys():
            self.gender = Gender(data["color"])
        if "lifespan" in data.keys():
            self.lifespan = data["lifespan"]
        if "iteration" in data.keys():
            self.iteration = data["iteration"]
        if "name" in data.keys():
            self.name = data["name"]

    def _add_data(self, data):
        """ add FS individual data """
        if data:
            self.living = data["living"]
            for x in data["names"]:
                if x["preferred"]:
                    self.name = Name(x)
                    break
                self.name = Name(x)
            if "gender" in data:
                if data["gender"]["type"] == "http://gedcomx.org/Male":
                    self.gender = Gender.Male
                elif data["gender"]["type"] == "http://gedcomx.org/Female":
                    self.gender = Gender.Female
            if "display" in data:
                if "lifespan" in data["display"]:
                    self.lifespan = data["display"]["lifespan"]
