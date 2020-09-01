from .name import Name
from enum import Enum
from typing import Optional


class Gender(Enum):
    Male = 1
    Female = 2
    Unknown = 3


class Individual:

    counter = 0

    def __init__(self, data, iteration):
        Individual.counter += 1
        self.num = Individual.counter
        self.fid: str = data["id"]
        self.name: Optional[Name]
        self.gender: Gender = Gender.Unknown
        self.living: bool = False
        self.iteration: int = iteration
        self.lifespan: str
        self._add_data(data)

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
