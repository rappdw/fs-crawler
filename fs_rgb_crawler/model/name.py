class Name:
    """ GEDCOM Name class
        :param data: FS Name data
    """

    def __init__(self, data=None):
        self.given = ""
        self.surname = ""
        self.prefix = None
        self.suffix = None
        if data:
            if "parts" in data["nameForms"][0]:
                for z in data["nameForms"][0]["parts"]:
                    if z["type"] == "http://gedcomx.org/Given":
                        self.given = z["value"]
                    if z["type"] == "http://gedcomx.org/Surname":
                        self.surname = z["value"]
                    if z["type"] == "http://gedcomx.org/Prefix":
                        self.prefix = z["value"]
                    if z["type"] == "http://gedcomx.org/Suffix":
                        self.suffix = z["value"]
