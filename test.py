from swiftbitcask import SwiftCask, TOMBSTONE_VALUE

# Example usage
cask = SwiftCask("example")

print("Testing put and get")

for i in range(100):
    cask.put(f"fooiiix{i}", f"bariiix{i}")
print(cask.get("fooiiix23") == "bariiix23")
cask.close()

cask = SwiftCask("example")

print("Testing put and get (2)")
for i in range(100):
    cask.put(f"fooiiixx{i}", f"bariiixx{i}")

print(cask.get("fooiiix23") == "bariiix23", cask.get("fooiiix23"))
cask.close()

cask = SwiftCask("example")

print("Testing retrieval from merged file")

print(cask.get("fooiiixx23") == "bariiixx23", cask.get("fooiiixx23"))
print(cask.get("fooiiix23") == "bariiix23", cask.get("fooiiix23"))

print("Testing deletion")

cask.delete("fooiiixx23")
print(cask.get("fooiiixx23") == TOMBSTONE_VALUE, cask.get("fooiiixx23"))

cask.close()

cask = SwiftCask("example")

print(cask.get("fooiiixx23"))
# cask.close()
