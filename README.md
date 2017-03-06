This is a datasource for serving historical bitcoin prices. Its an example for accessing a sorted file asynchronously
 with vert.x. It created a partial index over the entire file and reads only parts of the file as needed.
  
Datafile
========

The data used for this datasource can be downloaded from [http://api.bitcoincharts.com/v1/csv/].
The file have to be unziped because gzip only supports streaming but no random access. 

The format of the datafiles is

    timestamp,price,volume

for example
    
    1313331280,10.400000000000,0.779000000000
    1488333458,1176.000000000000,0.400000000000

- timestamp is unix timestamp (in seconds!)
- price is a float (currency depends on the file you download)
- volume is a float 

Each line is terminated with a new line character (`\n`)

The length of the line can vary, depending on the value of price and volume.
