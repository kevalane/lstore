# Metadata columns
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
BASE_RID_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
TIMESTAMP_COLUMN = 4
TPS_COLUMN = 5
META_COLUMNS = 6

# Page directory indexes
PAGE_TYPE = 0
PAGE_NUM = 1
OFFSET = 2

# Bufferpool
MAX_PAGES = 16

# Page
PAGE_SIZE = 4096
DATA_SIZE = 8 # int64
STORAGE_OPTION = 'big'

# Merge
MERGE_FREQ = 64 # This can be threshold that we merge after, feel free to change name or value