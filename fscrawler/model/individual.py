from .name import Name


class Individual:

    counter = 0

    def __init__(self, fid=None):
        Individual.counter += 1
        self.num = Individual.counter
        self.fid = fid
        self.famc_fid = set()
        self.fams_fid = set()
        self.name = None
        self.gender = None
        self.living = None
        self.nicknames = set()
        self.birthnames = set()
        self.married = set()
        self.aka = set()
        self.hop = 0

    def add_data(self, data):
        """ add FS individual data """
        if data:
            self.living = data["living"]
            for x in data["names"]:
                if x["preferred"]:
                    self.name = Name(x)
                elif x["type"] == "http://gedcomx.org/Nickname":
                    self.nicknames.add(Name(x))
                elif x["type"] == "http://gedcomx.org/BirthName":
                    self.birthnames.add(Name(x))
                elif x["type"] == "http://gedcomx.org/AlsoKnownAs":
                    self.aka.add(Name(x))
                elif x["type"] == "http://gedcomx.org/MarriedName":
                    self.married.add(Name(x))
            if "gender" in data:
                if data["gender"]["type"] == "http://gedcomx.org/Male":
                    self.gender = "M"
                elif data["gender"]["type"] == "http://gedcomx.org/Female":
                    self.gender = "F"
                elif data["gender"]["type"] == "http://gedcomx.org/Unknown":
                    self.gender = "U"

    def add_fams(self, fams):
        """ add family fid (for spouse or parent)"""
        self.fams_fid.add(fams)

    def add_famc(self, famc):
        """ add family fid (for child) """
        self.famc_fid.add(famc)
