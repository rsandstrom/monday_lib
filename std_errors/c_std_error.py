
"""
    StdError class is used to create a single list of errors that we may return or log when there is a known issue
    because we declare the variable list as a class variable it is essentially a global static variable.
    usage
        StdError.get(0) returns 'Success'
        StdError.get(1001) returns 'Database Not Connected'
        StdError.get(9999) returns 'N/A'  since it was not in the list
"""





class StdError:
    ERR_UNABLE_TO_CONNECT = 1001

    list = {
        "0": "Success",
        "1000": "Database Errors",
        "1001": "Database Not Connected",
        "1500": "Unable to execute query",
        "2000": "monday Errors",
        "2020": "Monday, Error Downloading files",
        "2021": "Monday, No files to download",
        "3000": "Fishbowl Errors",
        "-1": "N/A"
    }

    """
        StdError.get(error_number) returns the standard error message or empty string.
    """
    @staticmethod
    def get(key):
        try:
            return StdError.list.get(str(key))
        except KeyError:
            return 'N/A'
