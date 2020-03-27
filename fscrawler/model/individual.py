from .name import Name


class Individual:

    counter = 0

    def __init__(self, fid=None):
        Individual.counter += 1
        self.num = Individual.counter
        self.fid = fid
        self.name = None
        self.gender = None
        self.hop = 0

    def add_data(self, data):
        """ add FS individual data """
        if data:
            for x in data["names"]:
                self.name = Name(x)
            if "gender" in data:
                if data["gender"]["type"] == "http://gedcomx.org/Male":
                    self.gender = "M"
                elif data["gender"]["type"] == "http://gedcomx.org/Female":
                    self.gender = "F"
                elif data["gender"]["type"] == "http://gedcomx.org/Unknown":
                    self.gender = "U"
