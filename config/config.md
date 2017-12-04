The configuration is a finding the right balance of tradeoffs:

    {
      "bitcoinFile" : "btceUSD.csv",
      "chunkSize": 4096,
      "partialIndexFactor" : 4
      "indexChunkSize" : 128,
      "statsInterval" : 15000,
    }

- ChunkSize: size of a chunk that is read from the file to read datapoints.
  - the larger the chunk
     - the longer reading a "request" of 2000 datapoints take
     - the more memory is consumed per request
     - the smaller the index gets (filesize / chunksize), 1 indexpoint per chunk
     - the faster the index can be created
  - the smaller the chunk
     - the faster a reading of 2000 datapoints gets
     - the less memory is consumed per request
     - the larger the index gets
     - the longer the index creation takes
- partialIndexFactor: the factor for how many index points should be created at inital start. The number of maximum
  indexpoints is divided by this factor. I.e. 10 means only 1/10 of all index points get created
  - the smaller the valuer, the longer the startup, the less refinements are required
  - the larger the value, the faster the startup, more refinements are required
     
- IndexChunkSize: size of a chunk to create an index point from, should be around 2xsize of 1 datapoint
- statsInterval: frequency to print index statistics
     
