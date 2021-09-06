import datetime as dt
import numpy as np
import sigmf
from sigmf import SigMFFile

from sigmfmongo import *

# suppose we have an complex timeseries signal
data = np.zeros(1024*1024, dtype=np.complex64)
print(data.size)

# write those samples to file in cf32_le
#data.tofile('example.sigmf-data')

# create the metadata
meta = SigMFFile(
    # data_file='example.sigmf-data', # extension is optional
    global_info = {
        SigMFFile.DATATYPE_KEY: 'cf32_le',
        SigMFFile.SAMPLE_RATE_KEY: 48000,
        SigMFFile.AUTHOR_KEY: 'jane.doe@domain.org',
        SigMFFile.DESCRIPTION_KEY: 'All zero example file.',
        SigMFFile.VERSION_KEY: sigmf.__version__,
    }
)

# create a capture key at time index 0
meta.add_capture(0, metadata={
    SigMFFile.FREQUENCY_KEY: 915000000,
    SigMFFile.DATETIME_KEY: dt.datetime.utcnow().isoformat()+'Z',
})

# add an annotation at sample 100 with length 200 & 10 KHz width
meta.add_annotation(100, 200, metadata = {
    SigMFFile.FLO_KEY: 914995000.0,
    SigMFFile.FHI_KEY: 915005000.0,
    SigMFFile.COMMENT_KEY: 'example annotation',
})

# check for mistakes & write to disk
assert meta.validate()
##meta.tofile('example.sigmf-meta') # extension is optional



from pymongo import MongoClient

# Connect to the server with the hostName and portNumber.
connection = MongoClient("localhost", 27017)

# Connect to the Database where the images will be stored.
database = connection['sigmf']

filename = "sigmf_file"

# Count the number of files with same name
query_result = database['fs']['files'].count_documents({"filename": filename})
if(query_result > 0):
    print(f"Warning: {query_result} files with same name exists, will proceed.")

# Create a SigMFMongo object and push into db
sigmf = SigMFMongo(database, meta.ordered_metadata(), filename)
sigmf.tomongo(data.tobytes())

# Load a SigMFMongo object from mongodb, will get the most recent version
sigmf = frommongo(database, filename)
print(sigmf.ordered_metadata())
read_data = sigmf.read_data()
print(read_data.size)