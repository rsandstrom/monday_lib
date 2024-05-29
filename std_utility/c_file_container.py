"""
Class that stores file info and can store a buffer, and wite a physical file
"""
import io
from urllib.parse import unquote
import os
import logging
from result.c_result import Result
from std_utility.c_utility import Utility


class FileContainer:
    def __init__(self, url: str = None,
                 filename: str = None,
                 file_ext: str = None,
                 file_path: str = None,
                 buffer: io.BytesIO =None):


        self.url = url
        if url is not None:
            self.url_decoded = unquote(url)
        else:
            self.url_decoded = None
        self.base = None
        self.name = filename
        self.ext = file_ext
        self.path = file_path
        self.save_file_path = file_path
        self.bytes_io = buffer

        if url is not None:
            self.name = Utility.get_filename_from_url(url)
            self.base = Utility.get_basename_from_url(url)
            self.ext = Utility.get_fileext_from_url(url)
            self.path = Utility.get_path_from_url(url)

        if url is None and file_path is not None:
            self.name = Utility.get_filename_from_path(file_path)
            self.base = Utility.get_basename_from_path(file_path)
            self.ext = Utility.get_fileext_from_path(file_path)
            self.path = Utility.get_path_from_path(file_path)

        if url is None and filename is not None:
            self.name = filename
            self.base = Utility.get_basename_from_path(filename)
            self.ext = Utility.get_fileext_from_path(filename)
            if file_ext is not None:
                self.ext = file_ext
            self.path = file_path

    def write_file(self):
        result = Result()
        if self.is_text:
            try:
                with open(self.physical_path, 'w') as file:
                    b = self.getbuffer()
                    retval = file.write(b.decode())
                    return Result(0, f"{retval}")
            except Exception as ex:
                logging.warning(ex)
                result = Result(-1, ex)
        else:
            try:
                with open(self.physical_path, 'wb') as file:
                    b = self.getbuffer()
                    file.write(b)
            except Exception as ex:
                logging.warning(ex)
                result = Result(-1, ex)

        return result

    def getbuffer(self):
        return self.bytes_io.getvalue()

    def delete(self) -> Result:
        try:
            os.remove(self.physical_path)
            if os.path.exists(self.physical_path):
                return Result(code=-1, message="File still exists")
            else:
                return Result(code=0, message="File deleted")
        except Exception as ex:
            logging.warning(ex)
            return Result(code=-1, message=f"Error: {ex}")

    @property
    def is_text(self):
        if self.ext is not None:
            if self.ext in ['.txt', '.text', '.log', '.csv']:
                return True
        return False

    @property
    def physical_path(self):
        if self.save_file_path is None:
            return self.name
        return os.path.join(self.save_file_path, self.name)

    @property
    def fullpath(self):
        if self.path is None:
            return self.name
        else:
            return f"{self.path}{self.name}"



def main():
    csv = """"a good test",b,c
             1,2,3
             4,5,6"""
    buffer = io.BytesIO(csv.encode())
    fc = FileContainer(filename='test.csv', buffer=buffer)
    import pandas

    pd = pandas.read_csv(fc.bytes_io, encoding='cp1252')
    print(pd.to_dict(orient='records'))
    pass

if __name__ == "__main__":
    main()