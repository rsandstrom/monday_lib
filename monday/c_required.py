from monday.c_column import Column


class RequiredElements:
    def __init__(self):
        self.groups: [str] = []
        self.columns: [Column] = []
        self.sub_columns: [Column] = []

