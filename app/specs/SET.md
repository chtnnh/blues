# SET

## Options

The SET command supports a set of options that modify its behavior:

NX -- Only set the key if it does not already exist.
XX -- Only set the key if it already exists.
IFEQ ifeq-value -- Set the key’s value and expiration only if its current value is equal to ifeq-value. If the key doesn’t exist, it won’t be created.
IFNE ifne-value -- Set the key’s value and expiration only if its current value is not equal to ifne-value. If the key doesn’t exist, it will be created.
IFDEQ ifeq-digest -- Set the key’s value and expiration only if the hash digest of its current value is equal to ifeq-digest. If the key doesn’t exist, it won’t be created. See the Hash Digest section below for more information.
IFDNE ifne-digest -- Set the key’s value and expiration only if the hash digest of its current value is not equal to ifne-digest. If the key doesn’t exist, it will be created. See the Hash Digest section below for more information.
GET -- Return the old string stored at key, or nil if key did not exist. An error is returned and SET aborted if the value stored at key is not a string.
EX seconds -- Set the specified expire time, in seconds (a positive integer).
PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds (a positive integer).
PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds (a positive integer).
KEEPTTL -- Retain the time to live associated with the key.
