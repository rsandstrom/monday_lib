"""
  Group: This class is used to store group members
"""


class Group:
    def __init__(self, group_id: str = None, group_name: str = None):
        """Store the group id and name

        Args:
            group_id: The Monday.com id for the group
            group_name: The Group name displayed on Monday.com
        """
        self.group_id = group_id
        self.group_name = group_name
