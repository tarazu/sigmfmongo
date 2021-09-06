from sigmf.sigmffile import SigMFFile, dtype_info
import gridfs
import numpy as np

class SigMFMongo:
    def __init__(self, db, metadata=None, data_file=None,
            global_info=None, skip_checksum=False):
        self._db = db
        self.data_file = data_file
        self._fs = gridfs.GridFSBucket(self._db)
        self.sigmf = SigMFFile(metadata, data_file=None,
            global_info=global_info, skip_checksum=skip_checksum)

    def ordered_metadata(self):
        return self.sigmf.ordered_metadata()

    def tomongo(self, data_bytes):
        grid_in = self._fs.open_upload_stream(
            self.data_file,
            metadata = self.sigmf.ordered_metadata())
        grid_in.write(data_bytes)
        grid_in.close()  # uploaded on close

    def frommongo(self, data_file, global_info=None, skip_checksum=False):
        grid_out = self._fs.open_download_stream_by_name(data_file)
        self.data_file = data_file
        self.sigmf = SigMFFile(grid_out.metadata, data_file=None,
            global_info=global_info, skip_checksum=skip_checksum)
        
    def read_data(self, count=-1, offset=0):
        grid_out = self._fs.open_download_stream_by_name(self.data_file)
        dtype = dtype_info(self.sigmf.get_global_field(SigMFFile.DATATYPE_KEY))
        data_type_in = dtype['sample_dtype']
        read_data = np.frombuffer(grid_out.read(), dtype=data_type_in, count=count, offset=offset)
        return read_data

def frommongo(db, data_file, global_info=None, skip_checksum=False):
    sigmfmongo = SigMFMongo(db)
    sigmfmongo.frommongo(data_file, global_info=global_info, skip_checksum=skip_checksum)
    return sigmfmongo
