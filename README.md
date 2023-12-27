# swiftbitcask

An implementation of the [BitCask algorithm](https://riak.com/assets/bitcask-intro.pdf) in Python.

BitCask is an append-only key value store.

Every time you run BitCask, a new append-only file is created. An in-memory data structure keeps track of the most recent value for each key in the key-value store.

The append-only file is used to append records until either your script finishes running or you close a cask. When you close a cask, a merge operation is performed to merge all cask files into one.

## Installation

You can only install `swiftbitcask` from source.

To install `swiftbitcask`, run:

```
git clone https://github.com/capjamesg/swiftbitcask
cd swiftbitcask
pip3 install -e .
```

## Quickstart

With `swiftbitcask`, you can:

- Create a cask
- Add items to a cask
- Retrieve items from a cask
- Remove items from a cask
- Close a data store

### Create a cask

```python
from swiftbitcask import SwiftCask

cask = SwiftCask("example")
```

### Add items to a cask

```python
cask.put("name", "james")
```

### Retrieve items from a cask

```python
cask.get("name")
```

### Remove items from a cask

```python
cask.delete("name")
```

### Close a cask

When you close a cask, you must re-open a new cask using the `Create a cask` instructions above. Closed casks cannot be modified without being explicitly re-opened.

```python
cask.close()
```

## License

This project is licensed under an [MIT license](LICENSE).
