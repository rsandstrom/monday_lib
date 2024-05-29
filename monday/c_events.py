"""
Event JSON is sent and is different for each event type.
"""

EVENT_UPDATE_COLUMN_VALUE = 'update_column_value'
EVENT_UPDATE_NAME = 'update_name'
EVENT_CREATE_ITEM = 'create_pulse'


class EventUnit:
    def __init__(self, data: dict = None):
        self.symbol = data.get('symbol')
        self.custom_unit = data.get('custom_unit')
        self.direction = data.get('direction')


class EventValue:
    def __init__(self, data: dict = None):
        self.value_type = None
        self.value = None
        self.unit = None

        if data is not None:
            self.value_type = list(data.keys())[0]

            self.value = data.get('name')

            if self.value is None:
                self.value = data.get('value')
                tmp = data.get('unit')
                if tmp is not None:
                    self.unit = EventUnit(data.get('unit'))


class UpdateColumnValue:
    def __init__(self, data: dict = None):
        self.type = data.get('type')
        self.value = EventValue(data.get('value'))
        self.previous_value = EventValue(data.get('previousValue'))
        self.user_id = data.get('userId')
        self.original_trigger_uuid = data.get('originalTriggerUuid')
        self.board_id = data.get('boardId')
        self.pulse_id = data.get('pulseId')
        self.pulse_name = data.get('pulseName')
        self.column_id = data.get('columnId')
        self.column_type = data.get('columnType')
        self.column_name = data.get('columnTitle')
        self.changedAt = data.get('changedAt')
        self.is_top_group = data.get('isTopGroup')
        self.app = data.get('app')
        self.trigger_time = data.get('triggerTime')
        self.subscription_id = data.get('subscriptionId')
        self.trigger_uuid = data.get('triggerUuid')
        self.parent_item_id = data.get('parentItemId')
        self.parent_item_board_id = data.get('parentItemBoardId')
        self.row_id = self.pulse_id
        self.name = self.value.value
        # if self.parent_item_board_id is not None:
        #     self.board_id = self.parent_item_board_id
        # if self.parent_item_id is not None:
        #     self.row_id = self.parent_item_id


    def as_dict(self):
        return self.__dict__


class UpdateName:
    def __init__(self, data: dict = None):
        self.type = data.get('type')
        self.value = MondayEvent.get_item(data.get('value'), ['name', ])
        self.previous_value = MondayEvent.get_item(data.get('previousValue'), ['name'])
        self.user_id = data.get('userId')
        self.original_trigger_uuid = data.get('originalTriggerUuid')
        self.board_id = data.get('boardId')
        self.pulse_id = data.get('pulseId')
        self.app = data.get('app')
        self.trigger_time = data.get('triggerTime')
        self.subscription_id = data.get('subscriptionId')
        self.trigger_uuid = data.get('triggerUuid')
        self.name = self.value
        self.row_id = self.pulse_id

    def as_dict(self):
        return self.__dict__


class NewItem:
    def __init__(self, data: dict = None):
        self.type = data.get('type')
        self.column_values = data.get('columnValues')
        self.user_id = data.get('userId')
        self.original_trigger_uuid = data.get('originalTriggerUuid')
        self.board_id = data.get('boardId')
        self.pulse_id = data.get('pulseId')
        self.pulse_name = data.get('pulseName')
        self.group_id = data.get('groupId')
        self.group_name = data.get('groupName')
        self.group_color = data.get('groupColor')
        self.is_top_group = data.get('isTopGroup')
        self.app = data.get('app')
        self.trigger_time = data.get('triggerTime')
        self.subscription_id = data.get('subscriptionId')
        self.trigger_uuid = data.get('triggerUuid')
        self.name = self.pulse_name
        self.row_id = self.pulse_id

        def as_dict(self):
            return self.__dict__


class MondayEvent:
    def __init__(self, data: dict = None):
        self.type = data.get('type')

        if self.type == EVENT_UPDATE_COLUMN_VALUE:
            self.data = UpdateColumnValue(data)

        if self.type == EVENT_UPDATE_NAME:
            self.data = UpdateName(data)

        if self.type == EVENT_CREATE_ITEM:
            self.data = NewItem(data)

    @staticmethod
    def get_item(data: dict = None,  items: [] = None):
        val = None
        for item in items:
            val = data.get(item)
            if val is None:
                return None
        return val
