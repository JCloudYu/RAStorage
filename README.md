# Random Access Storage - RAStorage #
This library is intended to provide a native tiny storage using schemes from modern file systems. Input data is divided into different 261-fixed-sized blocks ( each of which can store up to 255 bytes data segment ).

The library can store 2^32-1 blocks at max.

## Example ##
```javascript
const {RAStorage} = require('rastorage');
const storage = RAStorage.InitAtPath( 'path to the storage dir' );



let promises = [];
promises.push(storage.put(Buffer.from(data)));        // Insert data and returns the initial block id
promises.push(storage.set(10, Buffer.from(data)));  // Replace data began with block #10
promises.push(storage.del(20, Buffer.from(data)));  // Delete data began with block #20
promises.push(storage.get(1);                                 // Get data began with block #1

await Promise.all(promises);
```