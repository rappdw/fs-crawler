from .name import Name
from enum import Enum
from typing import Optional

class Gender(Enum):
    Male = 1
    Female = 2
    Unknown = 3

class Individual:

    counter = 0

    def __init__(self, fid: str = None):
        Individual.counter += 1
        self.num = Individual.counter
        self.fid: str = fid
        self.name: Optional[Name] = None
        self.gender: Gender = Gender.Unknown
        self.living: bool = False
        self.hop: int = 0

    def add_data(self, data):
        """ add FS individual data """
        if data:
            self.living = data["living"] == 'true'
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
