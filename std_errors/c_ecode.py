"""
Upgraded standard error codes
"""
import enum
from enum import Enum
from std_errors.c_codes import Codes

class Ecode():
    class Database():
        general_error = Codes(1000, 'Database, General Error')
        not_connected = Codes(1001, 'Database, Not Connected')
        unable_to_execute_query = Codes(1500, 'Database Unable to Execute query')

    class Monday():
        general_error = Codes(2000, 'Monday, General Error')
        downloading_files = Codes(2020, 'Monday, Unable to download files')
        no_files_to_download = Codes(2021, 'Monday, No files to download')

    class Network():
        success_200 = Codes(200, "Success")
        bad_request_400 = Codes(400, '400 Bad Request')
        not_found_404 = Codes(404, '404 Not Found')
        internal_server_error_500 = Codes(500, '500 Internal Server Error')
        service_unavailable_503 = Codes(503, '503 Service not available')
        gateway_timeout_error_504 = Codes(504, '504 Gateway Timeout Error')

    class Fishbowl():
        general_error = Codes(3000, 'Fishbowl, General Error')

    success = Codes(0, 'Success')

    # database_general_error = Codes(1000, 'Database, General Error')
    # database_not_connected = Codes(1001, 'Database, Not Connected')
    # database_unable_to_execute_query = Codes(1500, 'Database Unable to Execute query')
    # monday_general_error = Codes(2000, 'Monday, General Error')
    # monday_downloading_files = Codes(2020, 'Monday, Unable to download files')
    # monday_no_files_to_download = Codes(2021, 'Monday, No files to download')
    # fishbowl_general_error = Codes(3000, 'Fishbowl, General Error')

