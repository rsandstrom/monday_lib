

class QueryHelper:
    def __init__(self):
        pass

    @staticmethod
    def get_field_id(parent_board, column_name):
        f_id = None
        if column_name in parent_board.column_info_map:
            f_id = parent_board.column_info_map.get(column_name).id
        return f_id

    @staticmethod
    def get_field_ids(parent_board, fields):
        field_ids = []
        if fields is not None:
            for field in fields:
                if field in parent_board.column_info_map:
                    f_id = parent_board.column_info_map.get(field).id
                    field_ids.append(f_id)
                else:
                    field_ids.append(field)
        if len(field_ids) > 0:
            fields = field_ids

        return fields

    @staticmethod
    def get_group_ids(parent_board, groups):
        if isinstance(groups, str):
            groups = [groups]

        group_ids = []

        if groups is not None:
            for name in groups:
                if name in parent_board.group_map:
                    group_ids.append(parent_board.group_map.get(name))
                else:
                    group_ids.append(name)
            if len(group_ids) > 0:
                groups = group_ids

        if groups is None:
            groups = []
            for group_name, group_id in parent_board.group_map.items():
                groups.append(group_id)

        return groups

    @staticmethod
    def clean_query(data):
        cmd = ' '.join(data.split())
        cmd = cmd.replace('\n', '')
        return cmd

    @staticmethod
    def monday_format_list(items=None):
        """
        returns either a item or a list of items as a string for use with monday queries
        for example,
            ['apple', 'pear'] = "[\"apple\",  \"pear"\"]"
            'apple' = "apple"
            [123, 456] = "[123, 456]"
        Args:
            items:

        Returns:

        """
        if isinstance(items, list):
            items = str(items)
        if isinstance(items, str):
            items = items.replace("'", "\"")
        return items

    @staticmethod
    def gen_view(fields=None):
        view = 'column_values '
        if fields is not None:
            view = "column_values (ids: FIELDS)"
            fields = QueryHelper.monday_format_list(fields)
            view = view.replace('FIELDS', fields)
        return view

    @staticmethod
    def gen_row_ids(ids=None):
        return QueryHelper.monday_format_list(ids)

    @staticmethod
    def gen_groups(groups=None):
        group = 'groups '
        if groups is not None:
            group = 'groups (ids: GROUPS) '
            if isinstance(groups, list):
                groups = str(groups)
            if isinstance(groups, str):
                groups = groups.replace("'", "\"")
            group = group.replace("GROUPS", groups)
        return group

    @staticmethod
    def gen_limit(limit=None, page=None, parens=True):
        if limit is None or page is None:
            return ''
        if limit > 100:
            limit = 100
        if parens:
            return f"(limit:{limit})"
        else:
            return f" limit:{limit}"
