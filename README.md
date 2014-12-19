# keyvalued

A simple key-value store.

## why?

I needed a simple key-value store to synchronize data across instances of gunicorn for a very
busy application (up to 3.2 billion impressions per day), and didn't want to be fussed with redis
or memcached as they were both overkill for what was needed.

Also I have plans for using multicast sockets to broadcast updates lazily, that way the cache
is kept even more fresh across all workers.

It also has some features that as far as I know, redis does not really provide, such as automatic
expiry of old data.

## deployment

I recommend using [gaffer](http://gaffer.readthedocs.org/en/latest/) to manage keyvalued.  We use
gaffer to manage both gunicorn and keyvalued for the production application that keyvalued supports.

Use something like:

```
[process:keyvalued]
cmd = python3.4 keyvalued.py
cwd = /srv/keyvalued
```

## why is it written in python?

`dict` objects seem sufficiently well optimized.  On a sandy bridge machine with 1.5k cache requests
per second, `keyvalued` only takes 5% cpu, so it is plenty good enough.  Maximum theoretical performance
is not really relevant when your application is split across many physical nodes.

Besides that, all of the relevant parts have C optimizations, so there's not much to be gained from
writing something in C here, at least in my opinion.  If you disagree, well, you do not have to use
this software now, do you?
