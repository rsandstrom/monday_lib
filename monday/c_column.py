"""
  Columns: Stores the data from the Monday.com json response when asking the API for board columns

  Used by all cells to ensure that the cells have the column information to update or insert cells in a row
"""

import json

import logging
from std_utility.c_utility import Utility


class Column:
    def __init__(self, c_index=None, c_id=None, c_name=None, c_type=None, c_labels=None):
        self.label_map = None
        self.settings = None
        if c_labels is None:
            c_labels = []
        self.index = c_index
        self.id = c_id
        self.name = Utility.clean_name(c_name)
        self.type = c_type
        self.labels = c_labels

    @property
    def has_labels(self):
        """True if the cell has labels."""
        return len(self.labels) > 0

    def init(self, c_index, c_id, c_name, c_type, c_labels):
        """initialize the column

        Args:
            c_index: The position of the column from left to right
            c_id: The id of the column (needed for updates and inserts)
            c_name: The display name of the column
            c_type: The type of the column
            c_labels: Labels list if the column has multiple choices

        """
        self.index = c_index
        self.id = c_id
        self.name = c_name
        self.type = c_type
        self.labels = c_labels

    # initialize the column from json source
    def from_json(self, c_index=0, column_json: dict = None):
        """ initialize the column fromm a dictionary

        Args:
            c_index: The position of the column from left to right
            column_json: The json, most likely from the Monday.com json response for a board

        """
        if column_json is None:
            column_json = {}

        self.index = c_index
        self.id = column_json.get('id')
        self.name = Utility.clean_name(column_json.get('title'))
        self.type = column_json.get('type')
        self.labels = []


        # load the settings for the columns and get the column field names
        _settings = column_json.get('settings_str')
        if _settings is not None and _settings != '{}':
            try:
                _json_settings = json.loads(_settings)
                self.settings = _json_settings 
                the_labels = _json_settings.get('labels')
                if type(the_labels) is list:
                    the_labels = the_labels[0]
                if the_labels is not None:
                    for k, v in the_labels.items():
                        self.labels.append(v)

                if self.has_labels:
                    self.label_map = {}
                    labels = self.settings.get('labels')
                    if isinstance(labels, dict):
                        for k, v in labels.items():
                            self.label_map[v] = int(k)
                    if isinstance(labels, list):
                        for item in self.settings.get('labels'):
                            self.label_map[item.get('name')] = item.get('id')

            except Exception as ex:
                logging.error(ex)

    def valid(self, label):
        """True if the label is in the list of labels

        Args:
            label: value to test, could be a value ready for insert or update and need to check if it matches a label

        Returns: True or False
        """

        return label in self.labels
