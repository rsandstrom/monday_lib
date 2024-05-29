"""
Class containing handy functions for creating and working with maps
"""


class Maps:

    @staticmethod
    def add_to_map_array(multi_map, item, data):
        """
        create a map that always stores map items in a list
        these maps always return a list when looked up
        """
        if item in multi_map:
            x: [] = multi_map.get(item)
            x.append(data)
        else:
            multi_map[item] = [data]

    @staticmethod
    def multimap_get(multi_map, name, index=None):
        """
        returns a list of items from a map or a single item if indexed
        items would be indexed if there was more than one with the same name
        """
        x = multi_map.get(name)

        if x is None:
            return None

        if index is None:
            return x

        if x is not None and len(multi_map) >= index:
            return multi_map.get(name)[index]

        return None
